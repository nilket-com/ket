//! DC-296 (Codex 01a20716): decision maintenance's selected step, captured by a drain that times
//! out. The capture is taken in the drain's timeout branch, before the abort, without waiting,
//! and published after the join.
//!
//! The controls:
//! - a held certification step: its permit and activation, and its report;
//! - another step: a finished pass's end;
//! - the certification phases, selected in order by the pass's own build;
//! - a clean close: no capture, and the word cleared when the task ends.

use super::*;
use crate::network::data_server::child_writes::PauseStage;
use crate::network::decision_proposals::MaintenanceStep;

/// Two rows through the leader, then a floor jump that makes a generation target due.
async fn rows_and_a_due_target(cluster: &Cluster, leader: u32, descriptor: &TableSchemaDescriptor) {
	for id in 1..=2 {
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

/// Closes `node` past a 2 s drain bound; requires the decision maintenance timeout, and returns
/// its one capture.
async fn close_past_the_bound(mut node: WendelonNode) -> DrainCapture {
	node.handles
		.set_lifecycle_drain_bound_for_test(std::time::Duration::from_secs(2));
	let captures = node.handles.drain_captures_for_test();
	let error = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a bounded close")
		.expect_err("an interrupted close is not orderly");
	assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
	assert!(
		error.to_string().contains("decision maintenance"),
		"{error}"
	);
	let captures = captures.lock().unwrap().clone();
	let [("decision maintenance", capture)] = captures.as_slice() else {
		panic!("exactly decision maintenance's capture: {captures:?}");
	};
	assert_eq!(capture.deadline_ms_after_drain_start, 2_000, "{capture:?}");
	assert!(
		capture.observed_ms_after_drain_start >= capture.deadline_ms_after_drain_start,
		"observed at or after the deadline: {capture:?}"
	);
	assert_eq!(
		(capture.selected_call, capture.tier),
		(None, TierDetail::NotApplicable),
		"no LSM call and no tier detail for this service"
	);
	*capture
}

fn selected(capture: &DrainCapture) -> MaintenanceStep {
	match capture.maintenance {
		MaintenanceDetail::Selected(step, since) => {
			assert!(
				since > 0,
				"the step carries its selection time: {capture:?}"
			);
			step
		}
		other => panic!("a selected step: {other:?}"),
	}
}

/// A certification step held at its permit (before its activation) past the bound: the capture
/// names the certification step's entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certification_step_held_past_the_drain_bound_captures_its_entry() {
	let (mut cluster, leader, _descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let (reached, _never_released) =
		server.pause_coordinator_for_test(PauseStage::CertificationStep);
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("a certification step holds its permit");
	drop(server);
	let capture = close_past_the_bound(take_node(&mut cluster, leader)).await;
	assert_eq!(selected(&capture), MaintenanceStep::CertifyActivate);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A step that built and staged its candidate, held before its report past the bound: the
/// capture names the report, not the build before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certification_step_held_at_its_report_past_the_drain_bound_captures_the_report() {
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let (reached, _never_released) =
		server.pause_coordinator_for_test(PauseStage::CertificationBeforeReport);
	rows_and_a_due_target(&cluster, leader, &descriptor).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the leader's report is held after durable staging");
	drop(server);
	let capture = close_past_the_bound(take_node(&mut cluster, leader)).await;
	assert_eq!(selected(&capture), MaintenanceStep::CertifyReport);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Another step: the owner held right after a finished pass past the bound. The capture names
/// the pass's end, not a certification phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_pass_held_at_its_end_past_the_drain_bound_captures_the_pass_end() {
	let time = crate::node::test_time::TestTime::new();
	let mut cluster = {
		let time = time.clone();
		Cluster::start_with(move |_, cfg| {
			cfg.test_time = Some(time.clone());
			None
		})
		.await
	};
	let _ = cluster.leader().await;
	let id = NODES[2];
	let wakes = time.wakes(id);
	let (entered, _never_released) = wakes.maintenance.hold_after_next_pass();
	let _ = wakes.maintenance.request();
	tokio::time::timeout(std::time::Duration::from_secs(30), entered.acquire())
		.await
		.expect("the owner reached the hold after a pass")
		.unwrap()
		.forget();
	let capture = close_past_the_bound(take_node(&mut cluster, id)).await;
	assert_eq!(selected(&capture), MaintenanceStep::PassEnd);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// The certification phases: a leader pass that builds, stages and reports a candidate selects
/// its entry, the closed-set read, the build, the stage and the report, in that order, then the
/// cleanup step.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_certifying_pass_selects_its_phases_in_order() {
	let (cluster, leader, descriptor) = ready().await;
	let word = cluster
		.node(leader)
		.handles
		.decision_maintenance_step_for_test()
		.expect("a ParentDecisions node has the word");
	word.record_for_test();
	rows_and_a_due_target(&cluster, leader, &descriptor).await;
	until_active(&cluster, 1).await;
	let trace = word.trace_for_test();
	let phases = [
		MaintenanceStep::CertifyActivate,
		MaintenanceStep::CertifyClosedSet,
		MaintenanceStep::CertifyBuild,
		MaintenanceStep::CertifyStage,
		MaintenanceStep::CertifyReport,
		MaintenanceStep::Cleanup,
	];
	assert!(
		trace.windows(phases.len()).any(|window| window == phases),
		"a certifying pass selects its phases in order: {trace:?}"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// A clean close: no capture is published, and the task's word is cleared when it ends (its
/// last selection was the stop winning at the loop's head).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_clean_close_captures_nothing_and_clears_the_step() {
	let (mut cluster, leader, _descriptor) = ready().await;
	let node = take_node(&mut cluster, leader);
	let word = node
		.handles
		.decision_maintenance_step_for_test()
		.expect("a ParentDecisions node has the word");
	assert!(
		word.selected().is_some(),
		"premise: the task selected steps"
	);
	word.record_for_test();
	let captures = node.handles.drain_captures_for_test();
	tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown())
		.await
		.expect("a bounded close")
		.expect("an orderly close");
	assert!(captures.lock().unwrap().is_empty(), "no drain timed out");
	assert_eq!(word.selected(), None, "cleared when the task ended");
	assert_eq!(
		word.trace_for_test().last(),
		Some(&MaintenanceStep::ProbeStop),
		"the stop won at the head, then the probe was joined"
	);
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}

/// Codex 01a2074a R1: a same-gid candidate this process already built and staged, whose report
/// was not recorded, is reused by the next pass: neither built nor staged again. Held at its
/// staged readback past the bound, the capture names the stage, and the build and staged-row
/// counts stay flat.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reused_candidate_held_at_its_staged_readback_captures_the_stage() {
	let (mut cluster, leader, descriptor) = ready().await;
	let server = cluster.node(leader).data_server.clone();
	let work = server.decision_peer_for_test().certification_work.clone();
	server
		.decision_peer_for_test()
		.skip_next_report
		.store(true, std::sync::atomic::Ordering::Release);
	let (reached, _never_released) =
		server.pause_coordinator_for_test(PauseStage::CertificationReusedReadback);
	rows_and_a_due_target(&cluster, leader, &descriptor).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the next pass reuses the candidate and reaches its staged readback");
	let held = work.snapshot();
	assert_eq!(
		held[0], 1,
		"premise: built once, by the earlier pass: {held:?}"
	);
	assert!(held[2] > 0, "premise: and staged then: {held:?}");
	assert!(
		!server
			.decision_peer_for_test()
			.skip_next_report
			.load(std::sync::atomic::Ordering::Acquire),
		"premise: the earlier pass signed a report and did not propose it"
	);
	drop(server);
	let capture = close_past_the_bound(take_node(&mut cluster, leader)).await;
	assert_eq!(selected(&capture), MaintenanceStep::CertifyStage);
	assert_eq!(work.snapshot(), held, "the reuse built and staged nothing");
	for node in cluster.nodes {
		let _ = tokio::time::timeout(std::time::Duration::from_secs(30), node.shutdown()).await;
	}
}
