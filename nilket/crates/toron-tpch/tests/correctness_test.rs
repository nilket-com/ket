//! ADR 273: the strict q06 correctness rail — the first answer-checked cell, kept separate from reachability.

use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron_tpch::telemetry::{Phase, Status};
use toron_tpch::{
	check_strict_q6, compare_strict_q6, correctness_matrix_from_log, q6_row_revenue,
	strict_q6_oracle,
};

// Toron's strict q06 executor output equals the independent `LineItemGenerator`-traversal oracle.
#[tokio::test]
async fn strict_q6_matches_independent_oracle() {
	let attempt = check_strict_q6(0.01).await;
	assert_eq!(attempt.query, "q06");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

// The oracle must select a non-zero row set at the default correctness scale — else a threshold bug could
// masquerade as a vacuous `0 == 0` pass.
#[test]
fn strict_q6_oracle_selects_nonempty_rows() {
	let (expected, matched) = strict_q6_oracle(0.01);
	assert!(matched > 0, "oracle matched {matched} rows at sf=0.01");
	assert!(
		expected.is_some(),
		"a non-empty match yields a Some(raw) sum"
	);
}

// The per-row oracle uses scaled `i128` arithmetic and the raw discount bound `5..=7` (`0.05..=0.07`) — not
// `f64`, and not the 100×-off `500..=700`.
#[test]
fn q6_row_revenue_uses_scaled_i128_bounds() {
	// In range: discount 5/7, quantity 1×100, a 1994 shipdate → Some(extendedprice_raw × discount_raw).
	assert_eq!(q6_row_revenue(10_000, 5, 100, 8_800), Some(50_000));
	assert_eq!(q6_row_revenue(10_000, 7, 100, 9_130), Some(70_000));
	// Discount outside `5..=7` (the 100×-off `500` would wrongly miss every real row).
	assert_eq!(q6_row_revenue(10_000, 4, 100, 8_800), None);
	assert_eq!(q6_row_revenue(10_000, 8, 100, 8_800), None);
	assert_eq!(q6_row_revenue(10_000, 500, 100, 8_800), None);
	// Quantity ≥ 24 (scaled 2400), and shipdate outside `[8766, 9131)`.
	assert_eq!(q6_row_revenue(10_000, 5, 2_400, 8_800), None);
	assert_eq!(q6_row_revenue(10_000, 5, 100, 8_765), None);
	assert_eq!(q6_row_revenue(10_000, 5, 100, 9_131), None);
}

// The comparator detects a wrong answer (else the rail could never fail): an exact raw match is `Ok`, an
// off-by-one raw value is `Err`, and a non-null output against an expected NULL is `Err`.
#[test]
fn compare_strict_q6_detects_raw_mismatch() {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"revenue",
		DataType::Decimal128(31, 4),
		true,
	)]));
	let array = Decimal128Array::from(vec![Some(123_456_i128)])
		.with_precision_and_scale(31, 4)
		.expect("decimal column");
	let batches = vec![RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("batch")];
	assert!(
		compare_strict_q6(&batches, Some(123_456)).is_ok(),
		"exact raw match"
	);
	assert!(
		compare_strict_q6(&batches, Some(123_457)).is_err(),
		"off-by-one raw is a mismatch"
	);
	assert!(
		compare_strict_q6(&batches, None).is_err(),
		"a non-null output against an expected NULL is a mismatch"
	);
}

// The correctness matrix is a separate rail: it renders `fail@compare` for a fail row, and IGNORES a
// reachability-suite row (so a reachability pass can't masquerade as a correctness check).
#[test]
fn correctness_matrix_is_separate_from_reachability() {
	let log = "{\"suite\":\"tpch_correctness\",\"query\":\"q06\",\"mode\":\"strict\",\"phase\":\"compare\",\"status\":\"fail\",\"reason\":\"raw mismatch\"}\n\
		{\"suite\":\"tpch\",\"query\":\"q01\",\"mode\":\"strict\",\"phase\":\"execute\",\"status\":\"pass\",\"reason\":null}\n";
	let rows = correctness_matrix_from_log(log);
	let q06 = rows
		.iter()
		.find(|row| row.starts_with("q06"))
		.expect("q06 row");
	assert!(
		q06.contains("strict=fail(raw mismatch@compare)"),
		"a mismatch renders as fail@compare: {q06}"
	);
	let q01 = rows
		.iter()
		.find(|row| row.starts_with("q01"))
		.expect("q01 row");
	assert!(
		q01.contains("strict=unchecked"),
		"a reachability-suite row must not leak into the correctness matrix: {q01}"
	);
}
