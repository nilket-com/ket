//! ADR 261/262 behavioral gates for the TPC-H reachability harness.

use toron_tpch::telemetry::{Phase, Status, reason_for_error, status_for_reason};
use toron_tpch::{
	ALL_QUERY_IDS, matrix_from_log, run_dev_q1, run_dev_q6, run_strict_q1, run_strict_q6,
	status_rows,
};

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

// ADR 263: strict Q6 (official-typed Decimal128 fixture, no user inputs) now REACHES execute — the
// Decimal ScalarValue slice coerces the comparison literals, widens `*`, and carries `SUM` through the
// Decimal128 path. The gauge moved `blocked(unsupported_decimal_scalar@bind)` → `pass@execute`.
// (Reconciles ADR 262's `strict_q6_blocks_at_bind_on_decimal`.)
#[tokio::test]
async fn strict_q6_reaches_execute() {
	let attempt = run_strict_q6(0.0001).await;
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
	assert_eq!(attempt.phase, Phase::Execute);
	assert!(
		attempt.digest.is_some(),
		"a pass carries a result digest: {}",
		attempt.human()
	);
	assert!(
		attempt.is_process_success(false),
		"a pass is success even without --allow-blocked"
	);
}

// ADR 266: q01 is attempted; its measured FIRST wall is `ambiguous aggregate projection` at PLAN —
// multiple same-function aggregates (four SUMs) the projection accounting can't disambiguate, reached
// BEFORE the decimal arithmetic bind. The decimal `+`/`-` slice (q01-nominated) is proven separately in
// decimal_probe; q01's true next blocker is the aggregate-projection gap (ADR 267). Same wall in dev and
// strict (it is type-independent), so this is the honest two-truths acceptance: the decimal slice passes,
// and full q01 reports its real first wall — not a guessed arithmetic/AVG blocker.
#[tokio::test]
async fn q01_attempts_and_blocks_at_aggregate_projection() {
	for attempt in [run_strict_q1(0.0001).await, run_dev_q1(0.0001).await] {
		assert_eq!(attempt.query, "q01");
		assert_eq!(attempt.status, Status::Blocked, "{}", attempt.human());
		assert_eq!(attempt.phase, Phase::Plan);
		assert_eq!(
			attempt.reason.as_deref(),
			Some("ambiguous_aggregate_projection"),
			"q01's first wall is multiple-same-function-aggregate projection (ADR 267): {}",
			attempt.human()
		);
	}
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

// ADR 262: `status` is static capability inventory, NOT the last measured result — it must not encode
// the strict `blocked` row (that lives only in the JSONL-backed matrix).
#[test]
fn status_marks_q06_strict_implemented_not_last_result() {
	let rows = status_rows();
	let q06 = rows
		.iter()
		.find(|row| row.starts_with("q06"))
		.expect("q06 row");
	assert!(
		q06.contains("strict=implemented"),
		"status is capability inventory: {q06}"
	);
	assert!(
		!q06.contains("blocked"),
		"status must not encode a measured failure: {q06}"
	);
}

// ADR 263: the matrix is regenerated from the JSONL — the measured rows come from there, not state.
// q06 now reads pass@execute in both modes; a blocked row (illustrative q01) still renders its phase.
#[test]
fn matrix_regenerates_from_jsonl() {
	let log = "{\"query\":\"q06\",\"mode\":\"dev\",\"phase\":\"execute\",\"status\":\"pass\",\"reason\":null}\n\
		{\"query\":\"q06\",\"mode\":\"strict\",\"phase\":\"execute\",\"status\":\"pass\",\"reason\":null}\n\
		{\"query\":\"q01\",\"mode\":\"strict\",\"phase\":\"bind\",\"status\":\"blocked\",\"reason\":\"unsupported_decimal_scalar\"}\n";
	let rows = matrix_from_log(log);
	let q06 = rows
		.iter()
		.find(|row| row.starts_with("q06"))
		.expect("q06 row");
	assert!(
		q06.contains("dev=pass@execute"),
		"matrix reflects the dev row: {q06}"
	);
	assert!(
		q06.contains("strict=pass@execute"),
		"ADR 263: strict q06 now reaches execute: {q06}"
	);
	let q01 = rows
		.iter()
		.find(|row| row.starts_with("q01"))
		.expect("q01 row");
	assert!(
		q01.contains("strict=blocked(unsupported_decimal_scalar@bind)"),
		"matrix still renders a blocked row's reason@phase: {q01}"
	);
}

#[test]
fn unrecognized_error_is_fail_not_blocked() {
	// The honesty invariant: only an allowlisted reason is a `blocked`; anything else is a `fail`,
	// so an unexpected engine error can never masquerade as an expected gap.
	assert_eq!(
		status_for_reason("unsupported_decimal_scalar"),
		Status::Blocked,
		"a recognized blocker is blocked"
	);
	assert_eq!(
		status_for_reason("some_unexpected_internal_error"),
		Status::Fail,
		"an unrecognized reason must be a fail, not a silent block"
	);
}

// ADR 262 review (seam 1): only the Decimal-vs-non-Decimal *comparison* shape maps to
// `unsupported_decimal_scalar`; an unrelated Decimal message that merely mentions "type" must not
// overclassify.
#[test]
fn reason_for_error_classifies_decimal_precisely() {
	assert_eq!(
		reason_for_error("comparison operands have different types: Decimal128(15, 2) and Float64"),
		"unsupported_decimal_scalar",
		"the measured strict bind gap"
	);
	assert_eq!(
		reason_for_error("unsupported CREATE TABLE column type: DECIMAL"),
		"unsupported_decimal_ddl",
		"the DDL gap"
	);
	assert_eq!(
		reason_for_error("unsupported decimal data type in storage"),
		"unsupported_decimal_arrow",
		"an unrelated Decimal message mentioning 'type' must NOT become the scalar gap"
	);
}
