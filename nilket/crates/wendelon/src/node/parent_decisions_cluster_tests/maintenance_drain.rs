//! DC-296: the decision maintenance pass across the maintenance stop.
//!
//! The loop checks the stop at its head, and a proposal's wait is cut by it (ADR 0707). A
//! certification build admitted before the stop observes it too: before it begins, between
//! its proofs and before it stages. Seeing it, the build returns nothing to report through
//! its step's ordinary path, so the lifecycle permit completes and the close is orderly.
//!
//! - a step admitted before the stop and released after it builds and stages nothing;
//! - a build stopped between two children's proofs proves nothing more and stages nothing;
//! - a step already at its report when the stop comes is not disturbed (it builds nothing
//!   more and submits nothing).
//!
//! The ordinary pass with no stop is `maintenance_step::a_certifying_pass_selects_its_phases_in_order`.

use super::*;
use crate::network::data_server::child_writes::PauseStage;

const PROMPT_CLOSE: std::time::Duration = std::time::Duration::from_secs(10);

/// `rows` single-row writes through the leader, then a floor jump that makes a generation
/// target due.
async fn rows_then_a_due_target(
	cluster: &Cluster,
	leader: u32,
	descriptor: &TableSchemaDescriptor,
	rows: i64,
) {
	for id in 1..=rows {
		assert_committed(
			&write_through(
				cluster,
				leader,
				write_meta(descriptor, 1, uuid_v7()),
				named_rows(descriptor, &[(id, "a")]),
			)
			.await,
			"a row",
		);
	}
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	jump_floor(cluster, now + 60_000).await;
}

/// Whether any remaining member applied `leader`'s report for the pending target.
async fn report_applied_anywhere(cluster: &Cluster, leader: u32) -> bool {
	for node in &cluster.nodes {
		let applied = node
			.state_machine
			.read_state(move |state| {
				state
					.decisions
					.as_ref()
					.and_then(|decisions| decisions.generations().pending.as_ref())
					.is_some_and(|pending| pending.reports.contains_key(&leader))
			})
			.await;
		if applied {
			return true;
		}
	}
	false
}

fn clean_shutdown_recorded(cluster: &Cluster, leader: u32) -> bool {
	cluster.configs[&leader]
		.data_dir
		.join("CLEAN_SHUTDOWN")
		.exists()
}

/// The leader's step is held at its permit while a target waits for its report. The
/// maintenance stop is set and seen, and only then is the step released. It builds nothing
/// and stages nothing; its permit completes and the close is orderly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certification_step_admitted_before_the_stop_builds_nothing_after_it() {
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let work = server.decision_peer_for_test().certification_work.clone();
	let mut stop = server.decision_maintenance_stop();
	let (mut reached, mut release) =
		server.pause_coordinator_for_test(PauseStage::CertificationStep);
	rows_then_a_due_target(&cluster, leader, &descriptor, 2).await;
	// Hold the first step that has a pending target without the leader's report.
	let deadline = tokio::time::Instant::now() + BOUND;
	let gid = loop {
		tokio::time::timeout_at(deadline, reached.notified())
			.await
			.expect("a certification step holds its permit");
		let generations = generations_at_leader(&cluster).await;
		if let Some(pending) = generations
			.pending
			.as_ref()
			.filter(|pending| !pending.reports.contains_key(&leader))
		{
			break pending.target.gid;
		}
		let next = server.pause_coordinator_for_test(PauseStage::CertificationStep);
		release.notify_one();
		(reached, release) = next;
	};
	let before = work.snapshot();
	drop(server);
	let node = take_node(&mut cluster, leader);
	let closing = tokio::spawn(node.shutdown());
	tokio::time::timeout(
		std::time::Duration::from_secs(5),
		stop.wait_for(|stopping| *stopping),
	)
	.await
	.expect("the stop is visible")
	.unwrap();
	assert_eq!(
		work.snapshot(),
		before,
		"held at its permit: nothing built before the release"
	);
	release.notify_one();
	let result = tokio::time::timeout(std::time::Duration::from_secs(30), closing)
		.await
		.expect("a bounded close")
		.unwrap();
	let after = work.snapshot();
	let (clean, reported) = (
		clean_shutdown_recorded(&cluster, leader),
		report_applied_anywhere(&cluster, leader).await,
	);
	eprintln!(
		"DC296 admitted-before-stop gid={gid} work (builds, base loaded, staged rows, activations, activation rows) before={before:?} after={after:?} close={result:?} CLEAN_SHUTDOWN={clean} report applied={reported}"
	);
	result.expect("an orderly close");
	assert_eq!(
		after, before,
		"a step released after the stop builds and stages nothing for gid {gid}"
	);
	assert!(
		clean,
		"its permit completed: the close recorded the directory clean"
	);
	assert!(!reported, "no report is submitted after the stop");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The build has begun and is held at the boundary between two children's proofs when the stop
/// comes. On release it proves nothing more (the next boundary is never reached), stages
/// nothing and reports nothing; its permit completes and the close is orderly.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_build_stopped_between_two_proofs_proves_nothing_more_and_stages_nothing() {
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let work = server.decision_peer_for_test().certification_work.clone();
	let mut stop = server.decision_maintenance_stop();
	let (reached, release) =
		server.pause_coordinator_for_test(PauseStage::CertificationBetweenProofs);
	rows_then_a_due_target(&cluster, leader, &descriptor, 4).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("a build is held between two children's proofs");
	let held = work.snapshot();
	assert!(held[0] >= 1, "premise: the build had begun: {held:?}");
	// Reached only if the build goes on to its next child after the release.
	let (next_reached, next_release) =
		server.pause_coordinator_for_test(PauseStage::CertificationBetweenProofs);
	drop(server);
	let node = take_node(&mut cluster, leader);
	let mut closing = tokio::spawn(node.shutdown());
	tokio::time::timeout(
		std::time::Duration::from_secs(5),
		stop.wait_for(|stopping| *stopping),
	)
	.await
	.expect("the stop is visible")
	.unwrap();
	release.notify_one();
	let mut went_on = false;
	let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
		loop {
			tokio::select! {
				done = &mut closing => break done.unwrap(),
				() = next_reached.notified(), if !went_on => {
					went_on = true;
					next_release.notify_one();
				}
			}
		}
	})
	.await
	.expect("a bounded close");
	let after = work.snapshot();
	let (clean, reported) = (
		clean_shutdown_recorded(&cluster, leader),
		report_applied_anywhere(&cluster, leader).await,
	);
	eprintln!(
		"DC296 stopped-between-proofs work held={held:?} after={after:?} next proof boundary reached={went_on} close={result:?} CLEAN_SHUTDOWN={clean} report applied={reported}"
	);
	assert!(
		!went_on,
		"no proof after the stop: the next boundary is never reached"
	);
	result.expect("an orderly close");
	assert_eq!(after, held, "nothing more is built and nothing is staged");
	assert!(
		clean,
		"its permit completed: the close recorded the directory clean"
	);
	assert!(!reported, "no report is submitted after the stop");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The leader's step had already built and durably staged its candidate, and is held before
/// proposing its report, when the stop comes. On release it submits nothing, builds nothing
/// more, and the close is prompt and orderly: a step past its build is not disturbed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certification_step_at_its_report_when_the_stop_comes_builds_nothing_after_it() {
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let work = server.decision_peer_for_test().certification_work.clone();
	let mut stop = server.decision_maintenance_stop();
	let (reached, release) =
		server.pause_coordinator_for_test(PauseStage::CertificationBeforeReport);
	rows_then_a_due_target(&cluster, leader, &descriptor, 2).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the leader's report is held after durable staging");
	let held = work.snapshot();
	assert!(
		held[0] >= 1 && held[2] >= 1,
		"premise: the candidate was built and staged before the stop: {held:?}"
	);
	drop(server);
	let node = take_node(&mut cluster, leader);
	let began = tokio::time::Instant::now();
	let closing = tokio::spawn(node.shutdown());
	tokio::time::timeout(
		std::time::Duration::from_secs(5),
		stop.wait_for(|stopping| *stopping),
	)
	.await
	.expect("the stop is visible")
	.unwrap();
	release.notify_one();
	let result = tokio::time::timeout(std::time::Duration::from_secs(30), closing)
		.await
		.expect("a bounded close")
		.unwrap();
	let took = began.elapsed();
	let after = work.snapshot();
	let (clean, reported) = (
		clean_shutdown_recorded(&cluster, leader),
		report_applied_anywhere(&cluster, leader).await,
	);
	eprintln!(
		"DC296 at-report work held={held:?} after={after:?} took={took:?} close={result:?} CLEAN_SHUTDOWN={clean} report applied={reported}"
	);
	result.expect("an orderly close");
	assert!(took < PROMPT_CLOSE, "the close took {took:?}");
	assert_eq!(after, held, "nothing is built or staged after the stop");
	assert!(clean, "the close recorded the directory clean");
	assert!(
		!reported,
		"a report reached after the stop is never submitted"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}
