//! ADR 261 behavioral gates for the TPC-H reachability harness.

use toron_tpch::telemetry::{Phase, Status, status_for_reason};
use toron_tpch::{ALL_QUERY_IDS, run_dev_q6, run_strict_q6, status_rows};

#[tokio::test]
async fn dev_q6_reaches_execute() {
	// Tiny scale factor — reachability, not volume.
	let attempt = run_dev_q6(0.0001).await;
	assert_eq!(
		attempt.status,
		Status::Pass,
		"dev Q6 should reach execute: {}",
		attempt.human()
	);
	assert_eq!(attempt.phase, Phase::Execute);
	assert!(attempt.digest.is_some(), "a pass carries a result digest");
	assert_eq!(attempt.query, "q06");
}

#[test]
fn status_lists_all_22() {
	let rows = status_rows();
	assert_eq!(rows.len(), 22);
	for id in ALL_QUERY_IDS {
		assert!(
			rows.iter().any(|row| row.starts_with(id)),
			"status inventory must list {id}"
		);
	}
}

#[test]
fn unrecognized_error_is_fail_not_blocked() {
	// The honesty invariant: only an allowlisted reason is a `blocked`; anything else is a `fail`,
	// so an unexpected engine error can never masquerade as an expected gap.
	assert_eq!(
		status_for_reason("unsupported_decimal_ddl"),
		Status::Blocked,
		"a recognized blocker is blocked"
	);
	assert_eq!(
		status_for_reason("some_unexpected_internal_error"),
		Status::Fail,
		"an unrecognized reason must be a fail, not a silent block"
	);
}

#[test]
fn strict_without_inputs_is_not_attempted() {
	let attempt = run_strict_q6();
	assert_eq!(attempt.status, Status::NotAttempted);
	assert_eq!(attempt.reason.as_deref(), Some("strict_inputs_required"));
	// `not_attempted` is a successful process outcome (nothing was claimed and nothing failed).
	assert!(attempt.is_process_success(false));
}
