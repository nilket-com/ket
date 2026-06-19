//! ADR 261/262 behavioral gates for the TPC-H reachability harness.

use toron_tpch::telemetry::{Phase, Status, reason_for_error, status_for_reason};
use toron_tpch::{
	ALL_QUERY_IDS, matrix_from_log, parse_scale_factor, run_dev_q1, run_dev_q3, run_dev_q05,
	run_dev_q6, run_dev_q10, run_dev_q14, run_strict_q1, run_strict_q3, run_strict_q05,
	run_strict_q6, run_strict_q10, run_strict_q14, status_rows,
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

// ADR 268: AVG(Decimal) now binds and executes, so strict q01 reaches execute end-to-end. q01's body is
// four SUMs, three AVGs, COUNT, GROUP BY, ORDER BY over `Decimal128(15,2)`; with 263's Decimal SUM/`*`,
// 266's Decimal `+`/`-`, and now 268's Decimal AVG, every arithmetic piece resolves. dev q01 was already
// pass@execute on `Float64`. (Supersedes ADR 267's `q01_advances_past_aggregate_projection`, which measured
// the prior `unsupported_decimal_avg@bind` wall.)
#[tokio::test]
async fn q01_reaches_execute_in_both_modes() {
	let dev = run_dev_q1(0.0001).await;
	assert_eq!(dev.query, "q01");
	assert_eq!(
		dev.status,
		Status::Pass,
		"dev q01 reaches execute: {}",
		dev.human()
	);
	assert_eq!(dev.phase, Phase::Execute);
	assert!(
		dev.digest.is_some(),
		"a pass carries a digest: {}",
		dev.human()
	);

	let strict = run_strict_q1(0.0001).await;
	assert_eq!(strict.query, "q01");
	assert_eq!(
		strict.status,
		Status::Pass,
		"268 clears AVG(Decimal); strict q01 now reaches execute: {}",
		strict.human()
	);
	assert_eq!(strict.phase, Phase::Execute);
	assert!(
		strict.digest.is_some(),
		"a pass carries a digest: {}",
		strict.human()
	);
}

// q14 (the decimal-ratio query) reaches execute in both modes: dev over Float64, strict over Decimal128.
#[tokio::test]
async fn q14_executes_in_both_modes() {
	assert!(
		status_rows()
			.iter()
			.any(|row| row == "q14 dev=implemented strict=implemented"),
		"q14 is an implemented capability in the static inventory"
	);

	for (mode, attempt) in [
		("dev", run_dev_q14(0.0001).await),
		("strict", run_strict_q14(0.0001).await),
	] {
		assert_eq!(attempt.query, "q14");
		assert_eq!(
			attempt.status,
			Status::Pass,
			"q14 {mode} reaches execute: {}",
			attempt.human()
		);
		assert_eq!(attempt.phase, Phase::Execute);
		assert!(
			attempt.digest.is_some(),
			"a pass carries a digest: {}",
			attempt.human()
		);
	}
}

// ADR 270: comma-join recognition keys q14's join, so it no longer rides a cartesian and the ADR 269 scale
// cap is gone — q14 runs at the CLI default scale the capped version refused. At sf=0.01 a keyless cartesian
// would be ~120M rows and never return, so completing here is itself proof the join is keyed.
#[tokio::test]
async fn q14_runs_keyed_at_default_scale() {
	let dev = run_dev_q14(0.01).await;
	assert_eq!(
		dev.status,
		Status::Pass,
		"q14 dev reaches execute at the default scale via a keyed (non-cartesian) join: {}",
		dev.human()
	);
	assert_eq!(dev.phase, Phase::Execute);
	assert!(
		dev.digest.is_some(),
		"a pass carries a digest: {}",
		dev.human()
	);
}

// q03 (the first 3-table query) reaches execute in both modes: dev over Float64, strict over Decimal128.
#[tokio::test]
async fn q03_executes_in_both_modes() {
	assert!(
		status_rows()
			.iter()
			.any(|row| row == "q03 dev=implemented strict=implemented"),
		"q03 is an implemented capability in the static inventory"
	);
	for (mode, attempt) in [
		("dev", run_dev_q3(0.0001).await),
		("strict", run_strict_q3(0.0001).await),
	] {
		assert_eq!(attempt.query, "q03");
		assert_eq!(
			attempt.status,
			Status::Pass,
			"q03 {mode} reaches execute: {}",
			attempt.human()
		);
		assert_eq!(attempt.phase, Phase::Execute);
		assert!(
			attempt.digest.is_some(),
			"a pass carries a digest: {}",
			attempt.human()
		);
	}
}

// q03's 3-table comma join is keyed, not a cartesian: at sf=0.01 a cartesian would exceed 10^12 rows.
#[tokio::test]
async fn q03_runs_keyed_at_default_scale() {
	let dev = run_dev_q3(0.01).await;
	assert_eq!(
		dev.status,
		Status::Pass,
		"q03 dev reaches execute at the default scale via a keyed (non-cartesian) join: {}",
		dev.human()
	);
	assert_eq!(dev.phase, Phase::Execute);
}

// ADR 274/275: q10 — the first 4-table query — reaches execute in both modes: dev over Float64, strict over
// Decimal128. ADR 275 added Decimal128 group/partition keys (encode_partition_key_value, GroupValue,
// group_value, group_column), flipping strict q10 from blocked(unsupported_decimal_group_key@execute) to
// pass@execute.
//
// Runs on an explicit large stack: q10's 4-table execution future overflows libtest's default test-thread
// stack — even on a multi-thread runtime (`block_on` polls the root future inline on the test thread) and with
// a raised `RUST_MIN_STACK` (which libtest's test thread does not honor). The CLI runs q10 fine on the main
// thread's larger stack, as does production.
#[test]
fn q10_executes_in_both_modes() {
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(async {
				assert!(
					status_rows()
						.iter()
						.any(|row| row == "q10 dev=implemented strict=implemented"),
					"q10 is an implemented capability in the static inventory"
				);
				for (mode, attempt) in [
					("dev", run_dev_q10(0.0001).await),
					("strict", run_strict_q10(0.0001).await),
				] {
					assert_eq!(attempt.query, "q10");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q10 {mode} reaches execute: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute);
					assert!(
						attempt.digest.is_some(),
						"a pass carries a digest: {}",
						attempt.human()
					);
				}
			});
		})
		.expect("spawn q10 thread")
		.join()
		.expect("q10 thread panicked");
}

// ADR 277: q05 — the first 6-table query and the first cyclic join graph (the supplier edge carries multiple
// equalities: l_suppkey=s_suppkey plus c_nationkey=s_nationkey). Both modes reach execute — the multi-key/
// cyclic join recognition handled it. The sf=0.01 dev check proves the join is keyed: a 6-table cartesian
// would be astronomically large and never complete, so finishing there is itself the proof. Runs on the
// explicit large-stack thread (a 6-table future is deeper than q10's 4-table, which already overflowed
// libtest's default test-thread stack).
#[test]
fn q05_executes_in_both_modes() {
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			runtime.block_on(async {
				assert!(
					status_rows()
						.iter()
						.any(|row| row == "q05 dev=implemented strict=implemented"),
					"q05 is an implemented capability in the static inventory"
				);
				for (mode, attempt) in [
					("dev", run_dev_q05(0.0001).await),
					("strict", run_strict_q05(0.0001).await),
				] {
					assert_eq!(attempt.query, "q05");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q05 {mode} reaches execute: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute);
					assert!(
						attempt.digest.is_some(),
						"a pass carries a digest: {}",
						attempt.human()
					);
				}
				let dev_default = run_dev_q05(0.01).await;
				assert_eq!(
					dev_default.status,
					Status::Pass,
					"q05 dev reaches execute at the default scale via a keyed (non-cartesian) join: {}",
					dev_default.human()
				);
				assert_eq!(dev_default.phase, Phase::Execute);
			});
		})
		.expect("spawn q05 thread")
		.join()
		.expect("q05 thread panicked");
}

// ADR 274: the Decimal group/partition-key error classifies precisely (not the generic Decimal-carrier
// bucket) — for both the measured exchange-partition-key message and the group_value message.
#[test]
fn decimal_group_key_error_classifies_precisely() {
	assert_eq!(
		reason_for_error(
			"InvalidExpression { message: \"unsupported exchange partition key type: Decimal128(15, 2)\" }"
		),
		"unsupported_decimal_group_key"
	);
	assert_eq!(
		reason_for_error("Kernel { message: \"unsupported GROUP BY type: Decimal128(15, 2)\" }"),
		"unsupported_decimal_group_key"
	);
}

// CLI hygiene (ADR 261 boundary): a `--sf` value must be a finite, positive number. Malformed / zero /
// negative / NaN / infinite is a usage error (the CLI maps it to exit 2 with no telemetry row) — degenerate
// scales never accumulate as `reachability.jsonl` rows.
#[test]
fn sf_parsing_rejects_nonpositive_and_malformed() {
	assert_eq!(
		parse_scale_factor("0.01"),
		Ok(0.01),
		"a normal scale parses"
	);
	assert_eq!(
		parse_scale_factor("0.0001"),
		Ok(0.0001),
		"the gauge scale parses"
	);
	for bad in ["nope", "0", "-1", "NaN", "nan", "inf", "-inf", "infinity"] {
		assert!(
			parse_scale_factor(bad).is_err(),
			"--sf {bad:?} must be a usage error, not a silently-coerced scale"
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
