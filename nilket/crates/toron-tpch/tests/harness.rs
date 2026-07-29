//! ADR 261/262 behavioral gates for the TPC-H reachability harness.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tpchgen::generators::{PartGenerator, PartSuppGenerator, SupplierGenerator};

use toron_tpch::telemetry::{Phase, Status, reason_for_error, status_for_reason};
use toron_tpch::{
	ALL_QUERY_IDS, Q11_DEV_SQL, Q11Scale, build_perf_query_spec, build_persisted_perf_query_spec,
	check_strict_q11, check_strict_query_with_data_source, compare_strict_q21, matrix_from_log,
	parse_scale_factor, q11_sql, q15_supplier_revenue_join_is_keyed,
	q18_grouped_membership_join_is_keyed, q19_lineitem_part_join_is_keyed, q21_oracle_from_rows,
	run_dev_q1, run_dev_q02, run_dev_q3, run_dev_q04, run_dev_q05, run_dev_q6, run_dev_q07,
	run_dev_q08, run_dev_q09, run_dev_q10, run_dev_q11, run_dev_q12, run_dev_q13, run_dev_q14,
	run_dev_q15, run_dev_q16, run_dev_q17, run_dev_q18, run_dev_q19, run_dev_q20, run_dev_q21,
	run_dev_q22, run_strict_q1, run_strict_q02, run_strict_q3, run_strict_q04, run_strict_q05,
	run_strict_q6, run_strict_q07, run_strict_q08, run_strict_q09, run_strict_q10, run_strict_q11,
	run_strict_q12, run_strict_q13, run_strict_q14, run_strict_q15, run_strict_q16, run_strict_q17,
	run_strict_q18, run_strict_q19, run_strict_q20, run_strict_q21, run_strict_q22, status_rows,
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

// ADR 294: q07's derived `shipping` subquery has a comma-list FROM body. Both modes now reach execute at
// the default scale, which proves the derived comma join is keyed rather than materialized as a cartesian.
#[test]
fn q07_executes_in_both_modes() {
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
						.any(|row| row == "q07 dev=implemented strict=implemented"),
					"q07 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q07(0.01).await),
					("strict", run_strict_q07(0.01).await),
				] {
					assert_eq!(attempt.query, "q07");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q07 {mode} reaches execute: {}",
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
		.expect("spawn q07 thread")
		.join()
		.expect("q07 thread panicked");
}

// ADR 297: q08 is q07's derived self-join shape plus `part` and `region`, with an outer grouped ratio.
// Completing at the default scale is the practical keyed-join tripwire for the eight-factor derived body.
#[test]
fn q08_executes_in_both_modes() {
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
						.any(|row| row == "q08 dev=implemented strict=implemented"),
					"q08 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q08(0.01).await),
					("strict", run_strict_q08(0.01).await),
				] {
					assert_eq!(attempt.query, "q08");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q08 {mode} reaches execute: {}",
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
		.expect("spawn q08 thread")
		.join()
		.expect("q08 thread panicked");
}

// ADR 301/302: q09's six-table product-profit query reaches execute in both modes at the default scale. Dev
// proves the derived comma join is keyed; strict proves Decimal add/sub aligns the two profit terms.
#[test]
fn q09_executes_in_both_modes() {
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
						.any(|row| row == "q09 dev=implemented strict=implemented"),
					"q09 is an implemented capability in the static inventory"
				);

				let dev = run_dev_q09(0.01).await;
				assert_eq!(dev.query, "q09");
				assert_eq!(
					dev.status,
					Status::Pass,
					"q09 dev reaches execute at the default scale via keyed joins: {}",
					dev.human()
				);
				assert_eq!(dev.phase, Phase::Execute);
				assert!(
					dev.digest.is_some(),
					"a pass carries a digest: {}",
					dev.human()
				);

				let strict = run_strict_q09(0.01).await;
				assert_eq!(strict.query, "q09");
				assert_eq!(
					strict.status,
					Status::Pass,
					"q09 strict reaches execute after Decimal add/sub operand alignment: {}",
					strict.human()
				);
				assert_eq!(strict.phase, Phase::Execute);
				assert!(
					strict.digest.is_some(),
					"a pass carries a digest: {}",
					strict.human()
				);
			});
		})
		.expect("spawn q09 thread")
		.join()
		.expect("q09 thread panicked");
}

// ADR 284: q02 exercises a five-table outer comma join plus a correlated scalar whose inner side is a
// four-table comma join. It runs on a large stack like q05/q10, and the default-scale checks prove the inner
// residual joins are keyed rather than materialized as a cartesian before the grouped MIN.
#[test]
fn q02_executes_in_both_modes() {
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
						.any(|row| row == "q02 dev=implemented strict=implemented"),
					"q02 is an implemented capability in the static inventory"
				);
				for (mode, attempt) in [
					("dev", run_dev_q02(0.0001).await),
					("strict", run_strict_q02(0.0001).await),
				] {
					assert_eq!(attempt.query, "q02");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q02 {mode} reaches execute: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute);
					assert!(
						attempt.digest.is_some(),
						"a pass carries a digest: {}",
						attempt.human()
					);
				}
				for (mode, attempt) in [
					("dev", run_dev_q02(0.01).await),
					("strict", run_strict_q02(0.01).await),
				] {
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q02 {mode} reaches execute at the default scale via keyed joins: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute);
				}
			});
		})
		.expect("spawn q02 thread")
		.join()
		.expect("q02 thread panicked");
}

// ADR 278/279: q04 — the gauge's first correlated-subquery query — reaches execute in both modes. ADR 279
// resolved its UNQUALIFIED correlated EXISTS ref (`l_orderkey = o_orderkey`) in the binder (catalog-aware,
// reusing the ADR 270 column_side machinery), flipping q04 from
// blocked(unsupported_correlated_exists@plan) to pass@execute. The official `+ INTERVAL '3' MONTH` bound is
// literal+literal (constant-folded). Both modes match (q04 uses no lineitem decimals).
#[tokio::test]
async fn q04_executes_in_both_modes() {
	assert!(
		status_rows()
			.iter()
			.any(|row| row == "q04 dev=implemented strict=implemented"),
		"q04 is an implemented capability in the static inventory"
	);
	for (mode, attempt) in [
		("dev", run_dev_q04(0.0001).await),
		("strict", run_strict_q04(0.0001).await),
	] {
		assert_eq!(attempt.query, "q04");
		assert_eq!(
			attempt.status,
			Status::Pass,
			"q04 {mode} reaches execute (the correlated EXISTS resolves): {}",
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

// ADR 290: q12's orders/lineitem comma join is keyed at the default scale, and the grouped CASE-count shape
// reaches execute in both modes. q12 uses no mode-specific Decimal columns.
#[tokio::test]
async fn q12_executes_in_both_modes_at_default_scale() {
	assert!(
		status_rows()
			.iter()
			.any(|row| row == "q12 dev=implemented strict=implemented"),
		"q12 is an implemented capability in the static inventory"
	);
	for (mode, attempt) in [
		("dev", run_dev_q12(0.01).await),
		("strict", run_strict_q12(0.01).await),
	] {
		assert_eq!(attempt.query, "q12");
		assert_eq!(
			attempt.status,
			Status::Pass,
			"q12 {mode} reaches execute at the default scale via a keyed join: {}",
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

// ADR 308: q13's right-side LEFT OUTER ON residual is pushed into `orders`, so the join stays key-only.
// The derived aggregate + outer join future is deep enough to need the same explicit stack as q05/q10.
#[test]
fn q13_executes_in_both_modes_at_default_scale() {
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
						.any(|row| row == "q13 dev=implemented strict=implemented"),
					"q13 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q13(0.01).await),
					("strict", run_strict_q13(0.01).await),
				] {
					assert_eq!(attempt.query, "q13");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q13 {mode} should reach execute after ON residual pushdown: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.phase,
						Phase::Execute,
						"q13 {mode}: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.digest.as_deref(),
						Some(
							"ArrowIpcDigest([6, 95, 246, 6, 27, 55, 213, 177, 25, 224, 9, 168, 103, 227, 170, 45, 253, 215, 34, 121, 181, 93, 71, 245, 175, 11, 37, 250, 13, 221, 8, 35])"
						),
						"q13 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.reason.is_none(),
						"a pass carries no blocker reason: {}",
						attempt.human()
					);
					assert!(
						attempt.is_process_success(false),
						"pass rows are successful without --allow-blocked"
					);
				}
			});
		})
		.expect("spawn q13 thread")
		.join()
		.expect("q13 thread panicked");
}

// ADR 318: q15's supplier/revenue comma equality must key even though `revenue` is an aggregate CTE.
#[test]
fn q15_executes_with_keyed_supplier_revenue_join() {
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
						.any(|row| row == "q15 dev=implemented strict=implemented"),
					"q15 is an implemented capability in the static inventory"
				);
				q15_supplier_revenue_join_is_keyed(false, 0.01)
					.await
					.expect("dev q15 supplier/revenue join is keyed");
				q15_supplier_revenue_join_is_keyed(true, 0.01)
					.await
					.expect("strict q15 supplier/revenue join is keyed");

				for (mode, attempt) in [
					("dev", run_dev_q15(0.01).await),
					("strict", run_strict_q15(0.01).await),
				] {
					assert_eq!(attempt.query, "q15");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q15 {mode} should execute once the supplier/revenue join is keyed: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute, "{}", attempt.human());
					assert!(attempt.reason.is_none(), "{}", attempt.human());
					assert!(
						attempt.digest.is_some(),
						"a passing row carries a digest: {}",
						attempt.human()
					);
					assert!(
						attempt.is_process_success(true),
						"pass rows are successful under --allow-blocked"
					);
					assert!(
						attempt.is_process_success(false),
						"pass rows are successful without --allow-blocked"
					);
				}
			});
		})
		.expect("spawn q15 thread")
		.join()
		.expect("q15 thread panicked");
}

// ADR 321/325/422: q11's Decimal x Int multiply reaches execute and the scale-owned literal keeps
// the FRACTION exact at the admitted scale.
#[test]
fn q11_executes_in_both_modes() {
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
						.any(|row| row == "q11 dev=implemented strict=implemented"),
					"q11 is an implemented capability in the static inventory"
				);

				for (mode, attempt, expected_digest) in [
					(
						"dev",
						run_dev_q11(0.01).await.expect("supported q11 scale"),
						"ArrowIpcDigest([178, 34, 129, 170, 233, 64, 153, 218, 48, 245, 72, 236, 149, 7, 191, 36, 14, 51, 182, 71, 190, 206, 104, 222, 135, 246, 4, 94, 161, 89, 87, 140])",
					),
					(
						"strict",
						run_strict_q11(0.01).await.expect("supported q11 scale"),
						"ArrowIpcDigest([201, 4, 26, 40, 73, 100, 188, 144, 117, 247, 19, 173, 137, 158, 222, 21, 181, 71, 171, 191, 115, 204, 213, 13, 222, 140, 97, 91, 228, 52, 95, 156])",
					),
				] {
					assert_eq!(attempt.query, "q11");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q11 {mode} should reach execute: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute, "{}", attempt.human());
					assert_eq!(
						attempt.digest.as_deref(),
						Some(expected_digest),
						"q11 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.reason.is_none(),
						"a pass carries no blocker reason: {}",
						attempt.human()
					);
					assert!(
						attempt.is_process_success(false),
						"pass rows are successful without --allow-blocked"
					);
				}
			});
		})
		.expect("spawn q11 thread")
		.join()
		.expect("q11 thread panicked");
}

fn q16_supplier_comment_excluded(comment: &str) -> bool {
	comment
		.find("Customer")
		.is_some_and(|idx| comment[idx + "Customer".len()..].contains("Complaints"))
}

#[test]
fn q16_default_fixture_is_nonempty_but_anti_join_is_not_live() {
	let scale_factor = 0.01;
	let sizes = HashSet::from([49, 14, 23, 45, 19, 3, 36, 9]);
	let parts: HashMap<i64, (String, String, i32)> = PartGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter_map(|part| {
			let brand = part.p_brand.to_string();
			let part_type = part.p_type.to_string();
			if brand == "Brand#45"
				|| part_type.starts_with("MEDIUM POLISHED")
				|| !sizes.contains(&part.p_size)
			{
				return None;
			}
			Some((part.p_partkey, (brand, part_type, part.p_size)))
		})
		.collect();
	let excluded_suppliers: HashSet<i64> = SupplierGenerator::new(scale_factor, 1, 1)
		.iter()
		.filter(|supplier| q16_supplier_comment_excluded(&supplier.s_comment))
		.map(|supplier| supplier.s_suppkey)
		.collect();
	assert!(
		excluded_suppliers.is_empty(),
		"q16's default fixture does not exercise the NOT IN exclusion branch; the correctness oracle must use a larger scale or synthetic"
	);

	let mut groups: HashMap<(String, String, i32), HashSet<i64>> = HashMap::new();
	for partsupp in PartSuppGenerator::new(scale_factor, 1, 1).iter() {
		if excluded_suppliers.contains(&partsupp.ps_suppkey) {
			continue;
		}
		let Some(group_key) = parts.get(&partsupp.ps_partkey) else {
			continue;
		};
		groups
			.entry(group_key.clone())
			.or_default()
			.insert(partsupp.ps_suppkey);
	}
	assert!(
		groups.values().any(|suppliers| !suppliers.is_empty()),
		"q16 default fixture should produce at least one result group"
	);
}

// ADR 311: q16's NOT IN anti-join stack no longer strands the partsupp/part comma equality above the cross
// recognizer. Running at the default scale is the anti-mask: the ADR 310 cartesian was multi-GB/minute-scale,
// while the keyed shape completes quickly with the same digest.
#[test]
fn q16_executes_in_both_modes_at_default_scale() {
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
						.any(|row| row == "q16 dev=implemented strict=implemented"),
					"q16 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q16(0.01).await),
					("strict", run_strict_q16(0.01).await),
				] {
					assert_eq!(attempt.query, "q16");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q16 {mode} should reach execute: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.phase,
						Phase::Execute,
						"q16 {mode}: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.digest.as_deref(),
						Some(
							"ArrowIpcDigest([164, 183, 72, 40, 46, 101, 81, 163, 233, 202, 133, 17, 254, 134, 141, 48, 25, 145, 205, 121, 138, 19, 238, 208, 235, 133, 137, 224, 90, 240, 14, 132])"
						),
						"q16 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.reason.is_none(),
						"a pass carries no blocker reason: {}",
						attempt.human()
					);
				}
			});
		})
		.expect("spawn q16 thread")
		.join()
		.expect("q16 thread panicked");
}

// ADR 282 resolved q17's unqualified correlated scalar. ADR 283 clears the strict Decimal operand-alignment
// walls behind it: non-literal Decimal comparison rescale and `SUM(decimal) / 7.0` literal coercion. q17 now
// reaches execute in both modes with the official SQL unchanged.
#[test]
fn q17_executes_in_both_modes() {
	// ADR 512: the default-on reachable-key reduction deepens q17's bind (graph
	// scratch re-binds over the engaged recombine); debug frames need the
	// explicit 64 MB stack, per the suite convention.
	std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(q17_executes_in_both_modes_body())
		})
		.expect("spawn q17 harness thread")
		.join()
		.expect("q17 harness thread panicked");
}

async fn q17_executes_in_both_modes_body() {
	assert!(
		status_rows()
			.iter()
			.any(|row| row == "q17 dev=implemented strict=implemented"),
		"q17 is an implemented capability in the static inventory"
	);

	let dev = run_dev_q17(0.0001).await;
	assert_eq!(dev.query, "q17");
	assert_eq!(
		dev.status,
		Status::Pass,
		"dev q17 reaches execute (the correlated scalar resolves): {}",
		dev.human()
	);
	assert_eq!(dev.phase, Phase::Execute);
	assert!(
		dev.digest.is_some(),
		"a pass carries a digest: {}",
		dev.human()
	);

	let strict = run_strict_q17(0.0001).await;
	assert_eq!(strict.query, "q17");
	assert_eq!(
		strict.status,
		Status::Pass,
		"strict q17 reaches execute after Decimal operand alignment: {}",
		strict.human()
	);
	assert_eq!(
		strict.phase,
		Phase::Execute,
		"strict q17: {}",
		strict.human()
	);
	assert!(
		strict.digest.is_some(),
		"a pass carries a digest: {}",
		strict.human()
	);
}

// ADR 322: q18 admits the grouped-IN body and keys the membership semi-join on the grouped output.
#[test]
fn q18_executes_with_keyed_grouped_membership_body() {
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
						.any(|row| row == "q18 dev=implemented strict=implemented"),
					"q18 is an implemented capability in the static inventory"
				);

				q18_grouped_membership_join_is_keyed(false, 0.01)
					.await
					.expect("dev q18 grouped-IN semi-join is keyed");
				q18_grouped_membership_join_is_keyed(true, 0.01)
					.await
					.expect("strict q18 grouped-IN semi-join is keyed");

				for (mode, attempt, expected_digest) in [
					(
						"dev",
						run_dev_q18(0.01).await,
						"ArrowIpcDigest([28, 48, 18, 202, 14, 6, 96, 104, 59, 127, 243, 219, 154, 38, 231, 39, 85, 146, 182, 89, 30, 20, 12, 135, 97, 59, 15, 38, 211, 89, 7, 103])",
					),
					(
						"strict",
						run_strict_q18(0.01).await,
						"ArrowIpcDigest([63, 211, 92, 198, 125, 214, 217, 115, 117, 24, 126, 202, 144, 247, 214, 60, 136, 143, 103, 186, 184, 77, 143, 255, 130, 163, 188, 63, 1, 220, 59, 23])",
					),
				] {
					assert_eq!(attempt.query, "q18");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q18 {mode} should execute through the keyed grouped-IN semi-join: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.phase,
						Phase::Execute,
						"q18 {mode}: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.digest.as_deref(),
						Some(expected_digest),
						"q18 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.reason.is_none(),
						"a pass carries no blocker reason: {}",
						attempt.human()
					);
					assert!(
						attempt.is_process_success(true),
						"q18 {mode} should succeed with --allow-blocked"
					);
					assert!(
						attempt.is_process_success(false),
						"q18 {mode} should be a normal process success"
					);
				}
			});
		})
		.expect("spawn q18 thread")
		.join()
		.expect("q18 thread panicked");
}

// ADR 319: the lineitem/part equality appears inside every q19 OR branch, and the binder extracts that
// common key before binding. The default-scale harness must prove the row is keyed, not a hidden cartesian.
#[test]
fn q19_executes_with_keyed_lineitem_part_join() {
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
						.any(|row| row == "q19 dev=implemented strict=implemented"),
					"q19 is an implemented capability in the static inventory"
				);

				q19_lineitem_part_join_is_keyed(false, 0.01)
					.await
					.expect("dev q19 lineitem/part join is keyed");
				q19_lineitem_part_join_is_keyed(true, 0.01)
					.await
					.expect("strict q19 lineitem/part join is keyed");

				for (mode, attempt) in [
					("dev", run_dev_q19(0.01).await),
					("strict", run_strict_q19(0.01).await),
				] {
					assert_eq!(attempt.query, "q19");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q19 {mode} should execute through the keyed join: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute, "{}", attempt.human());
					assert_eq!(
						attempt.reason.as_deref(),
						None,
						"q19 {mode} should not carry a blocker reason: {}",
						attempt.human()
					);
					assert!(
						attempt.digest.is_some(),
						"a pass row carries a digest: {}",
						attempt.human()
					);
					assert!(
						attempt.is_process_success(true),
						"q19 {mode} should succeed with --allow-blocked"
					);
					assert!(
						attempt.is_process_success(false),
						"q19 {mode} should be a normal process success"
					);
				}
			});
		})
		.expect("spawn q19 thread")
		.join()
		.expect("q19 thread panicked");
}

// ADR 286: q20's nested IN body and correlated scalar now compose with comparison-local numeric operand
// alignment (`ps_availqty > 0.5 * SUM(l_quantity)`), so both dev and strict modes reach execute.
#[test]
fn q20_executes_in_both_modes() {
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
						.any(|row| row == "q20 dev=implemented strict=implemented"),
					"q20 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q20(0.0001).await),
					("strict", run_strict_q20(0.0001).await),
				] {
					assert_eq!(attempt.query, "q20");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q20 {mode} should reach execute: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.phase,
						Phase::Execute,
						"q20 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.digest.is_some(),
						"a pass carries a digest: {}",
						attempt.human()
					);
				}
			});
		})
		.expect("spawn q20 thread")
		.join()
		.expect("q20 thread panicked");
}

#[test]
fn q21_oracle_counts_rows_and_discriminates_antijoins() {
	let oracle = q21_oracle_from_rows(
		[
			(1, "S1".to_string(), 10),
			(2, "S2".to_string(), 10),
			(3, "S3".to_string(), 11),
		],
		[
			(10, "SAUDI ARABIA".to_string()),
			(11, "MOROCCO".to_string()),
		],
		[
			(100, "F".to_string()),
			(101, "F".to_string()),
			(102, "F".to_string()),
			(103, "O".to_string()),
		],
		[
			(100, 1, 10, 20),
			(100, 1, 11, 21),
			(100, 2, 10, 10),
			(101, 1, 10, 20),
			(101, 2, 10, 20),
			(102, 1, 10, 20),
			(103, 3, 10, 20),
		],
	)
	.expect("q21 oracle");

	assert_eq!(
		oracle.contributing_rows, 2,
		"the oracle counts qualifying l1 rows"
	);
	assert_eq!(oracle.rows.len(), 1, "only S1 should qualify");
	assert_eq!(oracle.rows[0].s_name, "S1");
	assert_eq!(oracle.rows[0].numwait, 2);

	let schema = Arc::new(Schema::new(vec![
		Field::new("s_name", DataType::Utf8, false),
		Field::new("numwait", DataType::Int64, false),
	]));
	let batch = RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["S1"])),
			Arc::new(Int64Array::from(vec![2])),
		],
	)
	.expect("q21 record batch");

	compare_strict_q21(&[batch], &oracle).expect("q21 compare");
}

// ADR 323 clears q21's keyed semi/anti EXISTS residual wall; both modes now reach execute.
#[test]
fn q21_executes_in_both_modes() {
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
						.any(|row| row == "q21 dev=implemented strict=implemented"),
					"q21 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q21(0.01).await),
					("strict", run_strict_q21(0.01).await),
				] {
					assert_eq!(attempt.query, "q21");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q21 {mode} should reach execute: {}",
						attempt.human()
					);
					assert_eq!(attempt.phase, Phase::Execute, "{}", attempt.human());
					assert_eq!(
						attempt.reason.as_deref(),
						None,
						"q21 {mode} pass carries no blocker reason: {}",
						attempt.human()
					);
					assert!(
						attempt.digest.is_some(),
						"a passing q21 row carries a digest: {}",
						attempt.human()
					);
				}
			});
		})
		.expect("spawn q21 thread")
		.join()
		.expect("q21 thread panicked");
}

#[test]
fn reason_for_error_classifies_non_equi_exists_correlation_precisely() {
	assert_eq!(
		reason_for_error("unsupported non-equi EXISTS correlation without an equality key"),
		"unsupported_non_equi_exists_correlation",
		"non-equi-only EXISTS stays a precise, recognized blocker"
	);
	assert_eq!(
		status_for_reason("unsupported_non_equi_exists_correlation"),
		Status::Blocked,
		"the remaining non-equi-only boundary remains a recognized blocker"
	);
}

// ADR 305: SUBSTRING support clears q22's measured wall; both modes now reach execute.
#[test]
fn q22_executes_in_both_modes() {
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
						.any(|row| row == "q22 dev=implemented strict=implemented"),
					"q22 is an implemented capability in the static inventory"
				);

				for (mode, attempt) in [
					("dev", run_dev_q22(0.01).await),
					("strict", run_strict_q22(0.01).await),
				] {
					assert_eq!(attempt.query, "q22");
					assert_eq!(
						attempt.status,
						Status::Pass,
						"q22 {mode} should execute after SUBSTRING support: {}",
						attempt.human()
					);
					assert_eq!(
						attempt.phase,
						Phase::Execute,
						"q22 {mode}: {}",
						attempt.human()
					);
					assert!(
						attempt.digest.is_some(),
						"a pass carries a digest: {}",
						attempt.human()
					);
				}
			});
		})
		.expect("spawn q22 thread")
		.join()
		.expect("q22 thread panicked");
}

#[test]
fn reason_for_error_classifies_substring_precisely() {
	assert_eq!(
		reason_for_error("unsupported expression: Substring { expr: Identifier(c_phone) }"),
		"unsupported_substring"
	);
	assert_eq!(
		status_for_reason("unsupported_substring"),
		Status::Blocked,
		"the measured q22 wall is a recognized blocker"
	);
	assert_eq!(
		reason_for_error("unsupported expression: Function { name: lower }"),
		"unsupported_query_surface",
		"an unrelated unsupported expression must not become the substring wall"
	);
}

// ADR 278/279: a genuinely-unsupported correlated EXISTS (e.g. an ambiguous ref the binder can't orient)
// still classifies precisely as `unsupported_correlated_exists`, not the generic query-surface bucket.
#[test]
fn correlated_exists_error_classifies_precisely() {
	assert_eq!(
		reason_for_error("ambiguous correlated EXISTS column reference"),
		"unsupported_correlated_exists"
	);
}

#[test]
fn q07_derived_multi_source_error_classifies_precisely() {
	assert_eq!(
		reason_for_error("derived table subquery must have exactly one FROM source"),
		"unsupported_derived_multi_source"
	);
	assert_eq!(
		status_for_reason("unsupported_derived_multi_source"),
		Status::Blocked
	);
}

#[test]
fn reason_for_error_classifies_non_equi_join_on_precisely() {
	assert_eq!(
		reason_for_error("join ON only supports equality predicates"),
		"unsupported_non_equi_join_on"
	);
	assert_eq!(
		status_for_reason("unsupported_non_equi_join_on"),
		Status::Blocked,
		"the former q13 wall remains recognized for historical telemetry"
	);
	assert_eq!(
		reason_for_error("JOIN requires ON equality predicates"),
		"unrecognized",
		"the non-ON join-constraint guard is not q13's ON-residual wall"
	);
}

#[test]
fn reason_for_error_classifies_keyless_cross_join_precisely() {
	assert_eq!(
		reason_for_error(
			"UnsupportedExpression { message: \"CROSS JOIN requires a broadcast-eligible build side under the broadcast cap\" }"
		),
		"unsupported_cross_join"
	);
	assert_eq!(status_for_reason("unsupported_cross_join"), Status::Blocked);
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
fn q11_runtime_sql_is_owned_and_sf1_matches_the_golden_text() {
	let sf1 = Q11Scale::try_from_f64(1.0).expect("supported q11 scale");
	assert_eq!(q11_sql(sf1), Q11_DEV_SQL);

	let q11 = build_perf_query_spec("q11", "strict", 0.01).expect("q11 memory spec");
	assert!(
		matches!(&q11.sql, Cow::Owned(_)),
		"runtime q11 SQL is owned"
	);
	assert_eq!(
		q11.sql,
		q11_sql(Q11Scale::try_from_f64(0.01).expect("supported q11 scale"))
	);
	let root = std::env::temp_dir().join(format!("toron_tpch_adr422_sql_{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	let persisted =
		build_persisted_perf_query_spec("q11", "strict", 0.01, &root).expect("q11 persisted spec");
	assert!(matches!(&persisted.sql, Cow::Owned(_)));
	assert_eq!(persisted.sql, q11.sql);
	assert_eq!(persisted.export.sql, q11.sql);
	let _ = std::fs::remove_dir_all(&root);

	let q06 = build_perf_query_spec("q06", "strict", 0.01).expect("q06 memory spec");
	assert!(
		matches!(q06.sql, Cow::Borrowed(_)),
		"non-q11 SQL remains borrowed"
	);
}

#[tokio::test]
async fn q11_unsupported_scale_rejects_before_library_io() {
	let unsupported = 0.0001;
	assert!(run_dev_q11(unsupported).await.is_err());
	assert!(run_strict_q11(unsupported).await.is_err());
	assert!(check_strict_q11(unsupported).await.is_err());
	assert!(build_perf_query_spec("q11", "strict", unsupported).is_err());

	let root =
		std::env::temp_dir().join(format!("toron_tpch_adr422_reject_{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	assert!(build_persisted_perf_query_spec("q11", "strict", unsupported, &root).is_err());
	assert!(
		!root.exists(),
		"persisted spec rejection must precede dataset preparation"
	);
	assert!(
		check_strict_query_with_data_source("q11", unsupported, "parquet", &root)
			.await
			.is_err()
	);
	assert!(
		!root.exists(),
		"correctness rejection must precede persisted preparation"
	);
}

#[test]
fn q11_cli_rejections_are_exit2_without_telemetry_or_persisted_io() {
	let root = std::env::temp_dir().join(format!("toron_tpch_adr422_cli_{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	std::fs::create_dir_all(&root).expect("create isolated CLI cwd");
	let data_root = root.join("rejected_data");
	let exe = env!("CARGO_BIN_EXE_toron-tpch");
	let commands: Vec<Vec<String>> = vec![
		vec![
			"run", "--query", "q11", "--mode", "strict", "--sf", "0.0001",
		]
		.into_iter()
		.map(str::to_string)
		.collect(),
		vec![
			"correctness",
			"--query",
			"q11",
			"--sf",
			"0.0001",
			"--data-source",
			"memory",
		]
		.into_iter()
		.map(str::to_string)
		.collect(),
		vec![
			"perf-run-one",
			"--query",
			"q11",
			"--mode",
			"strict",
			"--sf",
			"0.0001",
			"--data-source",
			"memory",
		]
		.into_iter()
		.map(str::to_string)
		.collect(),
		vec![
			"perf-run-one".to_string(),
			"--query".to_string(),
			"q11".to_string(),
			"--mode".to_string(),
			"strict".to_string(),
			"--sf".to_string(),
			"0.0001".to_string(),
			"--data-source".to_string(),
			"parquet".to_string(),
			"--data-dir".to_string(),
			data_root.display().to_string(),
		],
	];
	for args in commands {
		let output = Command::new(exe)
			.current_dir(&root)
			.args(&args)
			.output()
			.expect("run toron-tpch rejection command");
		assert_eq!(
			output.status.code(),
			Some(2),
			"args={args:?} stdout={} stderr={}",
			String::from_utf8_lossy(&output.stdout),
			String::from_utf8_lossy(&output.stderr)
		);
		assert!(output.stdout.is_empty(), "args={args:?} emitted telemetry");
		assert!(
			String::from_utf8_lossy(&output.stderr)
				.contains("q11 requires an exactly supported scale"),
			"args={args:?}: {}",
			String::from_utf8_lossy(&output.stderr)
		);
	}
	assert!(!root.join("target/tpch/reachability.jsonl").exists());
	assert!(!root.join("target/tpch/correctness.jsonl").exists());
	assert!(
		!data_root.exists(),
		"unsupported persisted q11 prepared data"
	);
	let _ = std::fs::remove_dir_all(&root);
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

#[test]
fn reason_for_error_classifies_numeric_comparison_alignment_precisely() {
	assert_eq!(
		reason_for_error("comparison operands have different types: Int32 and Float64"),
		"unsupported_numeric_comparison_alignment",
		"q20 dev's historical second wall"
	);
	assert_eq!(
		reason_for_error("comparison operands have different types: Int32 and Decimal128(31, 4)"),
		"unsupported_numeric_comparison_alignment",
		"q20 strict's historical second wall"
	);
	assert_eq!(
		status_for_reason("unsupported_numeric_comparison_alignment"),
		Status::Blocked,
		"the precise q20 second wall is a recognized blocker"
	);
}
