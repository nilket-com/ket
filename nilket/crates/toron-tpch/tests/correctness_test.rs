//! ADR 273/276: the strict correctness rail — answer-checked cells (q06 scalar, q01 multi-row), kept separate
//! from reachability.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron_tpch::telemetry::{Phase, Status};
use toron_tpch::{
	Q1Group, Q3Group, assert_strict_q1_schema, assert_strict_q3_schema, avg_decimal_raw,
	check_strict_q1, check_strict_q3, check_strict_q6, compare_strict_q1, compare_strict_q3,
	compare_strict_q6, correctness_matrix_from_log, q6_row_revenue, strict_q1_oracle,
	strict_q3_oracle, strict_q6_oracle,
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

// ADR 276: Toron's strict q01 output (10 columns, N groups) equals the independent oracle — schema asserted,
// then every cell's raw i128 (sums + half-away averages) and count compared exactly.
#[tokio::test]
async fn strict_q1_matches_independent_oracle() {
	let attempt = check_strict_q1(0.01).await;
	assert_eq!(attempt.query, "q01");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

// The oracle is genuinely multi-row (the headline of q01 vs q06's scalar): > 1 group, ≥ 1 selected input row.
#[test]
fn strict_q1_oracle_selects_multiple_groups() {
	let oracle = strict_q1_oracle(0.01);
	let selected_rows: i64 = oracle.values().map(|group| group.count).sum();
	assert!(selected_rows >= 1, "selected {selected_rows} input rows");
	assert!(oracle.len() > 1, "expected > 1 group, got {}", oracle.len());
}

// avg_decimal_raw mirrors avg_decimal_finalize: half-away rounding, direction-aware via scaled.signum(), and
// FALLIBLE on impossible inputs (no harness panic).
#[test]
fn avg_decimal_raw_matches_half_away_and_is_fallible() {
	// Ties round away from zero in BOTH directions (out_precision is generous here).
	assert_eq!(avg_decimal_raw(5, 2, 0, 0, 19).unwrap(), 3); // 2.5 -> 3
	assert_eq!(avg_decimal_raw(-5, 2, 0, 0, 19).unwrap(), -3); // -2.5 -> -3
	assert_eq!(avg_decimal_raw(7, 2, 0, 0, 19).unwrap(), 4); // 3.5 -> 4
	assert_eq!(avg_decimal_raw(4, 2, 0, 0, 19).unwrap(), 2); // exact
	assert_eq!(avg_decimal_raw(7, 3, 0, 0, 19).unwrap(), 2); // 2.33 -> 2 (below the half)
	// Rescale from input scale 2 to output scale 6 (×10_000), as q01's averages do.
	assert_eq!(avg_decimal_raw(3, 2, 2, 6, 19).unwrap(), 15_000); // 30000 / 2
	// Impossible inputs are Err, not panics.
	assert!(avg_decimal_raw(5, 0, 0, 0, 19).is_err(), "zero count");
	assert!(
		avg_decimal_raw(5, 2, 6, 2, 19).is_err(),
		"output scale below input scale"
	);
	// The executor's final guard: a result that does not fit the output precision is Err (500 needs 3 digits).
	assert!(
		avg_decimal_raw(500, 1, 0, 0, 2).is_err(),
		"result exceeds output precision"
	);
}

// A single canonical q01 output row (1 group) matching the exact 10-column schema.
fn q1_batch(returnflag: &str, linestatus: &str, aggregates: [i128; 7], count: i64) -> RecordBatch {
	let decimal_types = [
		(15, 2),
		(15, 2),
		(32, 4),
		(38, 6),
		(19, 6),
		(19, 6),
		(19, 6),
	];
	let names = [
		"sum_qty",
		"sum_base_price",
		"sum_disc_price",
		"sum_charge",
		"avg_qty",
		"avg_price",
		"avg_disc",
	];
	let mut fields = vec![
		Field::new("l_returnflag", DataType::Utf8, false),
		Field::new("l_linestatus", DataType::Utf8, false),
	];
	for (name, (precision, scale)) in names.iter().zip(decimal_types) {
		fields.push(Field::new(
			*name,
			DataType::Decimal128(precision, scale),
			false,
		));
	}
	fields.push(Field::new("count_order", DataType::Int64, false));
	let mut columns: Vec<ArrayRef> = vec![
		Arc::new(StringArray::from(vec![returnflag])),
		Arc::new(StringArray::from(vec![linestatus])),
	];
	for (value, (precision, scale)) in aggregates.iter().zip(decimal_types) {
		columns.push(Arc::new(
			Decimal128Array::from(vec![*value])
				.with_precision_and_scale(precision, scale)
				.expect("decimal column"),
		));
	}
	columns.push(Arc::new(Int64Array::from(vec![count])));
	RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("q1 batch")
}

// The comparator detects a wrong answer: a consistent (output, oracle) pair matches; a one-cell perturbation
// of the oracle is fail@compare.
#[test]
fn q1_correctness_mismatch_is_fail_at_compare() {
	let count = 2_i64;
	let (sum_qty, sum_base, sum_discount) = (3_400_i128, 500_000_i128, 14_i128);
	let (sum_disc_price, sum_charge) = (49_000_000_i128, 5_292_000_000_i128);
	let batch = q1_batch(
		"A",
		"F",
		[
			sum_qty,
			sum_base,
			sum_disc_price,
			sum_charge,
			avg_decimal_raw(sum_qty, count, 2, 6, 19).unwrap(),
			avg_decimal_raw(sum_base, count, 2, 6, 19).unwrap(),
			avg_decimal_raw(sum_discount, count, 2, 6, 19).unwrap(),
		],
		count,
	);
	let batches = vec![batch];
	let key = ("A".to_string(), "F".to_string());
	let mut oracle = BTreeMap::new();
	oracle.insert(
		key.clone(),
		Q1Group {
			sum_qty,
			sum_base_price: sum_base,
			sum_disc_price,
			sum_charge,
			sum_discount,
			count,
		},
	);
	assert!(
		compare_strict_q1(&batches, &oracle).is_ok(),
		"a consistent (output, oracle) pair matches"
	);
	oracle.get_mut(&key).unwrap().sum_qty += 1;
	assert!(
		compare_strict_q1(&batches, &oracle).is_err(),
		"a one-cell oracle perturbation is fail@compare"
	);
}

// Schema-first: a drifted output type (sum_disc_price as Decimal128(31,4), not the measured (32,4)) is caught
// by the Phase-0 assertion BEFORE any value comparison.
#[test]
fn strict_q1_schema_drift_is_detected() {
	let fields = vec![
		Field::new("l_returnflag", DataType::Utf8, false),
		Field::new("l_linestatus", DataType::Utf8, false),
		Field::new("sum_qty", DataType::Decimal128(15, 2), false),
		Field::new("sum_base_price", DataType::Decimal128(15, 2), false),
		// Drift: the real type is Decimal128(32, 4).
		Field::new("sum_disc_price", DataType::Decimal128(31, 4), false),
		Field::new("sum_charge", DataType::Decimal128(38, 6), false),
		Field::new("avg_qty", DataType::Decimal128(19, 6), false),
		Field::new("avg_price", DataType::Decimal128(19, 6), false),
		Field::new("avg_disc", DataType::Decimal128(19, 6), false),
		Field::new("count_order", DataType::Int64, false),
	];
	let decimal = |precision: u8, scale: i8| -> ArrayRef {
		Arc::new(
			Decimal128Array::from(vec![0_i128])
				.with_precision_and_scale(precision, scale)
				.expect("decimal column"),
		)
	};
	let columns: Vec<ArrayRef> = vec![
		Arc::new(StringArray::from(vec!["A"])),
		Arc::new(StringArray::from(vec!["F"])),
		decimal(15, 2),
		decimal(15, 2),
		decimal(31, 4),
		decimal(38, 6),
		decimal(19, 6),
		decimal(19, 6),
		decimal(19, 6),
		Arc::new(Int64Array::from(vec![1_i64])),
	];
	let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("drift batch");
	let error = assert_strict_q1_schema(&[batch]).expect_err("schema drift must be detected");
	assert!(
		error.contains("sum_disc_price"),
		"the drift names the offending column: {error}"
	);
}

// ADR 276 review (Finding 1): a NULL output cell must be rejected (fail@compare), not compared through its
// garbage value slot. q01 aggregates over non-empty groups are always non-null, so a NULL is an anomaly.
#[test]
fn q1_null_cell_is_fail_at_compare() {
	let fields = vec![
		Field::new("l_returnflag", DataType::Utf8, false),
		Field::new("l_linestatus", DataType::Utf8, false),
		// sum_qty is nullable here so a NULL can be injected; the schema assert checks type, not nullability.
		Field::new("sum_qty", DataType::Decimal128(15, 2), true),
		Field::new("sum_base_price", DataType::Decimal128(15, 2), false),
		Field::new("sum_disc_price", DataType::Decimal128(32, 4), false),
		Field::new("sum_charge", DataType::Decimal128(38, 6), false),
		Field::new("avg_qty", DataType::Decimal128(19, 6), false),
		Field::new("avg_price", DataType::Decimal128(19, 6), false),
		Field::new("avg_disc", DataType::Decimal128(19, 6), false),
		Field::new("count_order", DataType::Int64, false),
	];
	let decimal = |precision: u8, scale: i8, value: Option<i128>| -> ArrayRef {
		Arc::new(
			Decimal128Array::from(vec![value])
				.with_precision_and_scale(precision, scale)
				.expect("decimal column"),
		)
	};
	let columns: Vec<ArrayRef> = vec![
		Arc::new(StringArray::from(vec!["A"])),
		Arc::new(StringArray::from(vec!["F"])),
		decimal(15, 2, None), // sum_qty is NULL
		decimal(15, 2, Some(0)),
		decimal(32, 4, Some(0)),
		decimal(38, 6, Some(0)),
		decimal(19, 6, Some(0)),
		decimal(19, 6, Some(0)),
		decimal(19, 6, Some(0)),
		Arc::new(Int64Array::from(vec![1_i64])),
	];
	let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("null batch");
	let mut oracle = BTreeMap::new();
	oracle.insert(
		("A".to_string(), "F".to_string()),
		Q1Group {
			count: 1,
			..Default::default()
		},
	);
	let error = compare_strict_q1(&[batch], &oracle).expect_err("a NULL cell must be fail@compare");
	assert!(
		error.contains("sum_qty") && error.contains("NULL"),
		"the failure names the null column: {error}"
	);
}

// ADR 280: a q03 output batch matching the exact 4-column strict schema, rows in the given emit order.
fn q3_batch(rows: &[(i64, i128, i32, i32)]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("l_orderkey", DataType::Int64, false),
		Field::new("revenue", DataType::Decimal128(32, 4), false),
		Field::new("o_orderdate", DataType::Date32, false),
		Field::new("o_shippriority", DataType::Int32, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|r| r.0).collect::<Vec<i64>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|r| r.1).collect::<Vec<i128>>())
					.with_precision_and_scale(32, 4)
					.expect("revenue decimal column"),
			),
			Arc::new(Date32Array::from(
				rows.iter().map(|r| r.2).collect::<Vec<i32>>(),
			)),
			Arc::new(Int32Array::from(
				rows.iter().map(|r| r.3).collect::<Vec<i32>>(),
			)),
		],
	)
	.expect("q3 batch")
}

fn q3_oracle(rows: &[(i64, i128, i32, i32)]) -> BTreeMap<i64, Q3Group> {
	rows.iter()
		.map(|&(orderkey, revenue, orderdate, shippriority)| {
			(
				orderkey,
				Q3Group {
					revenue,
					orderdate,
					shippriority,
				},
			)
		})
		.collect()
}

// ADR 280: Toron's strict q03 output (the first multi-table answer-check) equals the independent
// generator-traversal oracle — schema asserted, then the answer bijection + ORDER BY prefix verified.
#[tokio::test]
async fn strict_q3_matches_independent_oracle() {
	let attempt = check_strict_q3(0.01).await;
	assert_eq!(attempt.query, "q03");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

// The oracle is genuinely multi-table + multi-group: > 1 group and ≥ 1 contributing lineitem row.
#[test]
fn strict_q3_oracle_selects_multiple_groups() {
	let oracle = strict_q3_oracle(0.01);
	assert!(
		oracle.selected_rows >= 1,
		"selected {} contributing rows",
		oracle.selected_rows
	);
	assert!(
		oracle.groups.len() > 1,
		"expected > 1 group, got {}",
		oracle.groups.len()
	);
}

// The Phase-0 schema gate accepts the exact 4-column schema and rejects a drifted revenue precision (a
// (31,4)-vs-(32,4) slip would otherwise compare at the wrong place — the ADR 276 trap).
#[test]
fn strict_q3_output_schema_is_asserted() {
	assert!(assert_strict_q3_schema(&[q3_batch(&[(1, 100, 9300, 0)])]).is_ok());

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("l_orderkey", DataType::Int64, false),
			Field::new("revenue", DataType::Decimal128(31, 4), false),
			Field::new("o_orderdate", DataType::Date32, false),
			Field::new("o_shippriority", DataType::Int32, false),
		])),
		vec![
			Arc::new(Int64Array::from(vec![1_i64])),
			Arc::new(
				Decimal128Array::from(vec![100_i128])
					.with_precision_and_scale(31, 4)
					.expect("decimal"),
			),
			Arc::new(Date32Array::from(vec![9300_i32])),
			Arc::new(Int32Array::from(vec![0_i32])),
		],
	)
	.expect("drifted batch");
	assert!(
		assert_strict_q3_schema(&[drifted]).is_err(),
		"a (31,4) revenue must trip the schema gate"
	);
}

// The discriminating test for the design decision: when two groups tie on (revenue, o_orderdate), the
// comparator accepts EITHER emit order — it keys answers by the unique l_orderkey and only checks the ORDER BY
// prefix, never the executor's internal tiebreak.
#[test]
fn q3_comparator_accepts_either_tie_order() {
	let oracle = q3_oracle(&[(10, 500, 100, 0), (20, 500, 100, 7)]);
	assert!(
		compare_strict_q3(
			&[q3_batch(&[(10, 500, 100, 0), (20, 500, 100, 7)])],
			&oracle
		)
		.is_ok(),
		"tie order [10, 20] accepted"
	);
	assert!(
		compare_strict_q3(
			&[q3_batch(&[(20, 500, 100, 7), (10, 500, 100, 0)])],
			&oracle
		)
		.is_ok(),
		"tie order [20, 10] equally accepted"
	);
}

// A revenue ordering that violates ORDER BY revenue DESC is fail@compare even when every cell matches.
#[test]
fn q3_order_prefix_violation_is_fail() {
	let oracle = q3_oracle(&[(10, 100, 100, 0), (20, 200, 100, 0)]);
	assert!(
		compare_strict_q3(
			&[q3_batch(&[(10, 100, 100, 0), (20, 200, 100, 0)])],
			&oracle
		)
		.is_err(),
		"revenue ascending violates ORDER BY revenue DESC"
	);
}

// A single wrong cell is fail@compare (else the rail could never catch a wrong answer).
#[test]
fn q3_correctness_mismatch_is_fail_at_compare() {
	let oracle = q3_oracle(&[(10, 500, 100, 0), (20, 300, 100, 0)]);
	// Emit order keeps revenue DESC (501 then 300); orderkey 10's revenue is off by one.
	assert!(
		compare_strict_q3(
			&[q3_batch(&[(10, 501, 100, 0), (20, 300, 100, 0)])],
			&oracle
		)
		.is_err(),
		"an off-by-one revenue is a mismatch"
	);
}

// A duplicate emitted l_orderkey is rejected up front (it must not silently overwrite in the bijection map).
#[test]
fn q3_duplicate_output_key_is_fail_at_compare() {
	let oracle = q3_oracle(&[(10, 500, 100, 0), (20, 300, 100, 0)]);
	// Row count (2) matches the oracle, but orderkey 10 appears twice and 20 is absent.
	assert!(
		compare_strict_q3(
			&[q3_batch(&[(10, 500, 100, 0), (10, 500, 100, 0)])],
			&oracle
		)
		.is_err(),
		"a duplicate emitted l_orderkey is rejected"
	);
}
