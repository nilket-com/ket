//! The strict correctness rail — answer-checked cells, kept separate from reachability.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron_tpch::telemetry::{Phase, Status};
use toron_tpch::{
	DecimalRaw, Q1Group, Q02Oracle, Q02Row, Q3Group, Q04Oracle, Q5Group, Q07Oracle, Q08Oracle,
	Q09Oracle, Q10Oracle, Q10Row, Q12Group, Q12Oracle, Q14Oracle, Q17_CORRECTNESS_SCALE, Q17Oracle,
	Q20Oracle, Q20Row, assert_strict_q1_schema, assert_strict_q02_schema, assert_strict_q3_schema,
	assert_strict_q04_schema, assert_strict_q5_schema, assert_strict_q07_schema,
	assert_strict_q08_schema, assert_strict_q09_schema, assert_strict_q10_schema,
	assert_strict_q12_schema, assert_strict_q14_schema, assert_strict_q17_schema,
	assert_strict_q20_schema, avg_decimal_raw, check_strict_q1, check_strict_q02, check_strict_q3,
	check_strict_q04, check_strict_q5, check_strict_q6, check_strict_q07, check_strict_q08,
	check_strict_q09, check_strict_q10, check_strict_q12, check_strict_q14, check_strict_q17,
	check_strict_q20, compare_strict_q1, compare_strict_q02, compare_strict_q3, compare_strict_q04,
	compare_strict_q5, compare_strict_q6, compare_strict_q07, compare_strict_q08,
	compare_strict_q09, compare_strict_q10, compare_strict_q12, compare_strict_q14,
	compare_strict_q17, compare_strict_q20, correctness_matrix_from_log, decimal_division_raw,
	q02_type_matches_brass, q04_oracle_from_rows, q6_row_revenue, q07_contribution,
	q08_contribution, q08_market_share_raw, q09_amount_raw, q09_order_year,
	q09_part_name_contains_green, q09_partsupp_cost, q20_part_name_is_forest_prefix,
	q20_threshold_passes, strict_q1_oracle, strict_q02_oracle, strict_q3_oracle, strict_q04_oracle,
	strict_q5_oracle, strict_q6_oracle, strict_q07_oracle, strict_q08_oracle, strict_q09_oracle,
	strict_q10_oracle, strict_q12_oracle, strict_q14_oracle, strict_q17_oracle, strict_q20_oracle,
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

#[derive(Clone)]
struct NullableQ02Row {
	s_acctbal: Option<i128>,
	s_name: Option<String>,
	n_name: Option<String>,
	p_partkey: Option<i64>,
	p_mfgr: Option<String>,
	s_address: Option<String>,
	s_phone: Option<String>,
	s_comment: Option<String>,
}

fn q02_row(s_acctbal: i128, s_name: &str, n_name: &str, p_partkey: i64, p_mfgr: &str) -> Q02Row {
	Q02Row {
		s_acctbal,
		s_name: s_name.to_string(),
		n_name: n_name.to_string(),
		p_partkey,
		p_mfgr: p_mfgr.to_string(),
		s_address: format!("{s_name} address"),
		s_phone: format!("{s_name} phone"),
		s_comment: format!("{s_name} comment"),
	}
}

fn nullable_q02_row(row: &Q02Row) -> NullableQ02Row {
	NullableQ02Row {
		s_acctbal: Some(row.s_acctbal),
		s_name: Some(row.s_name.clone()),
		n_name: Some(row.n_name.clone()),
		p_partkey: Some(row.p_partkey),
		p_mfgr: Some(row.p_mfgr.clone()),
		s_address: Some(row.s_address.clone()),
		s_phone: Some(row.s_phone.clone()),
		s_comment: Some(row.s_comment.clone()),
	}
}

fn q02_batch(rows: &[Q02Row]) -> RecordBatch {
	let nullable_rows: Vec<NullableQ02Row> = rows.iter().map(nullable_q02_row).collect();
	q02_nullable_batch(&nullable_rows)
}

fn q02_nullable_batch(rows: &[NullableQ02Row]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("s_acctbal", DataType::Decimal128(15, 2), true),
		Field::new("s_name", DataType::Utf8, true),
		Field::new("n_name", DataType::Utf8, true),
		Field::new("p_partkey", DataType::Int64, true),
		Field::new("p_mfgr", DataType::Utf8, true),
		Field::new("s_address", DataType::Utf8, true),
		Field::new("s_phone", DataType::Utf8, true),
		Field::new("s_comment", DataType::Utf8, true),
	]));
	let strings = |f: fn(&NullableQ02Row) -> &Option<String>| {
		Arc::new(StringArray::from(
			rows.iter()
				.map(|row| f(row).as_deref())
				.collect::<Vec<Option<&str>>>(),
		)) as ArrayRef
	};
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.s_acctbal).collect::<Vec<_>>())
					.with_precision_and_scale(15, 2)
					.expect("q02 s_acctbal decimal column"),
			),
			strings(|row| &row.s_name),
			strings(|row| &row.n_name),
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.p_partkey).collect::<Vec<_>>(),
			)),
			strings(|row| &row.p_mfgr),
			strings(|row| &row.s_address),
			strings(|row| &row.s_phone),
			strings(|row| &row.s_comment),
		],
	)
	.expect("q02 batch")
}

fn q02_int32_partkey_batch() -> RecordBatch {
	let row = q02_row(100, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("s_acctbal", DataType::Decimal128(15, 2), true),
			Field::new("s_name", DataType::Utf8, true),
			Field::new("n_name", DataType::Utf8, true),
			Field::new("p_partkey", DataType::Int32, true),
			Field::new("p_mfgr", DataType::Utf8, true),
			Field::new("s_address", DataType::Utf8, true),
			Field::new("s_phone", DataType::Utf8, true),
			Field::new("s_comment", DataType::Utf8, true),
		])),
		vec![
			Arc::new(
				Decimal128Array::from(vec![Some(row.s_acctbal)])
					.with_precision_and_scale(15, 2)
					.expect("q02 acctbal"),
			),
			Arc::new(StringArray::from(vec![Some(row.s_name.as_str())])),
			Arc::new(StringArray::from(vec![Some(row.n_name.as_str())])),
			Arc::new(Int32Array::from(vec![Some(row.p_partkey as i32)])),
			Arc::new(StringArray::from(vec![Some(row.p_mfgr.as_str())])),
			Arc::new(StringArray::from(vec![Some(row.s_address.as_str())])),
			Arc::new(StringArray::from(vec![Some(row.s_phone.as_str())])),
			Arc::new(StringArray::from(vec![Some(row.s_comment.as_str())])),
		],
	)
	.expect("q02 int32 partkey batch")
}

fn q02_oracle(rows: Vec<Q02Row>) -> Q02Oracle {
	Q02Oracle {
		matched_parts: rows
			.iter()
			.map(|row| row.p_partkey)
			.collect::<std::collections::BTreeSet<_>>()
			.len(),
		rows,
		min_tie_parts: 0,
	}
}

#[test]
fn strict_q02_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q02(0.01))
		})
		.expect("spawn q02 correctness thread")
		.join()
		.expect("q02 correctness thread panicked");
	assert_eq!(attempt.query, "q02");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q02_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q02_oracle(0.01);
	assert!(
		oracle.matched_parts > 0,
		"matched parts: {}",
		oracle.matched_parts
	);
	assert!(
		!oracle.rows.is_empty(),
		"oracle rows: {}",
		oracle.rows.len()
	);
}

#[test]
fn strict_q02_output_schema_is_asserted() {
	assert!(
		assert_strict_q02_schema(&[q02_batch(&[q02_row(
			100,
			"Supplier#000000001",
			"GERMANY",
			10,
			"Manufacturer#1"
		)])])
		.is_ok()
	);
	assert!(
		assert_strict_q02_schema(&[q02_int32_partkey_batch()]).is_err(),
		"a non-measured p_partkey type must trip the schema gate"
	);
}

#[test]
fn q02_brass_is_suffix_match() {
	assert!(q02_type_matches_brass("ECONOMY ANODIZED BRASS"));
	assert!(q02_type_matches_brass("BRASS"));
	assert!(
		!q02_type_matches_brass("BRASS PLATED STEEL"),
		"LIKE '%BRASS' is a suffix match, not contains"
	);
	assert!(!q02_type_matches_brass("ECONOMY ANODIZED BRASSX"));
}

#[test]
fn q02_correctness_mismatch_is_fail_at_compare() {
	let row = q02_row(500, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	let oracle = q02_oracle(vec![row.clone()]);

	let mut wrong_acctbal = row.clone();
	wrong_acctbal.s_acctbal += 1;
	assert!(
		compare_strict_q02(&[q02_batch(&[wrong_acctbal])], &oracle).is_err(),
		"an off-by-one Decimal acctbal is a mismatch"
	);

	let mut wrong_string = row.clone();
	wrong_string.s_name.push_str("-wrong");
	assert!(
		compare_strict_q02(&[q02_batch(&[wrong_string])], &oracle).is_err(),
		"a wrong projected string is a mismatch"
	);

	let mut wrong_partkey = row.clone();
	wrong_partkey.p_partkey += 1;
	assert!(
		compare_strict_q02(&[q02_batch(&[wrong_partkey])], &oracle).is_err(),
		"a wrong p_partkey is a mismatch"
	);
}

#[test]
fn q02_dropped_min_tie_supplier_is_fail() {
	let row_a = q02_row(500, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	let row_b = q02_row(400, "Supplier#000000002", "GERMANY", 10, "Manufacturer#1");
	let mut oracle = q02_oracle(vec![row_a.clone(), row_b.clone()]);
	oracle.min_tie_parts = 1;
	assert!(
		compare_strict_q02(&[q02_batch(&[row_a])], &oracle).is_err(),
		"dropping one same-part min supplier must fail"
	);
}

#[test]
fn q02_extra_missing_or_duplicate_row_is_fail_at_compare() {
	let row_a = q02_row(500, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	let row_b = q02_row(400, "Supplier#000000002", "FRANCE", 20, "Manufacturer#2");
	let oracle = q02_oracle(vec![row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q02(&[q02_batch(&[row_a.clone()])], &oracle).is_err(),
		"a missing row is a mismatch"
	);
	let extra = q02_row(300, "Supplier#000000003", "FRANCE", 20, "Manufacturer#2");
	assert!(
		compare_strict_q02(
			&[q02_batch(&[row_a.clone(), row_b.clone(), extra])],
			&oracle
		)
		.is_err(),
		"an extra non-min row is a mismatch"
	);
	assert!(
		compare_strict_q02(&[q02_batch(&[row_a.clone(), row_a])], &oracle).is_err(),
		"an unexpected duplicate row is a mismatch"
	);
}

#[test]
fn q02_null_cell_is_fail_at_compare() {
	let row = q02_row(500, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	let oracle = q02_oracle(vec![row.clone()]);
	let mut cases: Vec<(&str, NullableQ02Row)> = Vec::new();

	let mut null_row = nullable_q02_row(&row);
	null_row.s_acctbal = None;
	cases.push(("s_acctbal", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.s_name = None;
	cases.push(("s_name", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.n_name = None;
	cases.push(("n_name", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.p_partkey = None;
	cases.push(("p_partkey", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.p_mfgr = None;
	cases.push(("p_mfgr", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.s_address = None;
	cases.push(("s_address", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.s_phone = None;
	cases.push(("s_phone", null_row));
	let mut null_row = nullable_q02_row(&row);
	null_row.s_comment = None;
	cases.push(("s_comment", null_row));

	for (column, row) in cases {
		assert!(
			compare_strict_q02(&[q02_nullable_batch(&[row])], &oracle).is_err(),
			"a NULL {column} is fail@compare"
		);
	}
}

#[test]
fn q02_order_violation_is_fail() {
	let row_a = q02_row(100, "Supplier#000000001", "GERMANY", 10, "Manufacturer#1");
	let row_b = q02_row(200, "Supplier#000000002", "GERMANY", 20, "Manufacturer#1");
	let oracle = q02_oracle(vec![row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q02(&[q02_batch(&[row_a, row_b])], &oracle).is_err(),
		"s_acctbal ascending violates ORDER BY s_acctbal DESC"
	);

	let row_a = q02_row(100, "Supplier#000000001", "ZAMBIA", 10, "Manufacturer#1");
	let row_b = q02_row(100, "Supplier#000000002", "ARGENTINA", 20, "Manufacturer#1");
	let oracle = q02_oracle(vec![row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q02(&[q02_batch(&[row_a, row_b])], &oracle).is_err(),
		"n_name descending within equal acctbal violates the ASC secondary key"
	);
}

fn q5_batch(rows: &[(&str, i128)]) -> RecordBatch {
	let nullable_rows: Vec<(Option<&str>, Option<i128>)> = rows
		.iter()
		.map(|&(name, revenue)| (Some(name), Some(revenue)))
		.collect();
	q5_nullable_batch(&nullable_rows)
}

fn q5_nullable_batch(rows: &[(Option<&str>, Option<i128>)]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("n_name", DataType::Utf8, true),
		Field::new("revenue", DataType::Decimal128(32, 4), true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.1).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(32, 4)
					.expect("q5 revenue decimal column"),
			),
		],
	)
	.expect("q5 batch")
}

fn q5_oracle(rows: &[(&str, i128)]) -> BTreeMap<String, Q5Group> {
	rows.iter()
		.map(|&(name, revenue)| (name.to_string(), Q5Group { revenue }))
		.collect()
}

#[test]
fn strict_q5_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q5(0.01))
		})
		.expect("spawn q5 correctness thread")
		.join()
		.expect("q5 correctness thread panicked");
	assert_eq!(attempt.query, "q05");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q5_oracle_selects_multiple_groups() {
	let oracle = strict_q5_oracle(0.01);
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

#[test]
fn strict_q5_output_schema_is_asserted() {
	assert!(assert_strict_q5_schema(&[q5_batch(&[("ARGENTINA", 100)])]).is_ok());

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("n_name", DataType::Utf8, false),
			Field::new("revenue", DataType::Decimal128(31, 4), false),
		])),
		vec![
			Arc::new(StringArray::from(vec!["ARGENTINA"])),
			Arc::new(
				Decimal128Array::from(vec![100_i128])
					.with_precision_and_scale(31, 4)
					.expect("decimal"),
			),
		],
	)
	.expect("drifted q5 batch");
	assert!(
		assert_strict_q5_schema(&[drifted]).is_err(),
		"a non-measured revenue precision must trip the schema gate"
	);
}

#[test]
fn q5_comparator_accepts_either_tie_order() {
	let oracle = q5_oracle(&[("ARGENTINA", 500), ("BRAZIL", 500)]);
	assert!(
		compare_strict_q5(&[q5_batch(&[("ARGENTINA", 500), ("BRAZIL", 500)])], &oracle).is_ok(),
		"tie order [ARGENTINA, BRAZIL] accepted"
	);
	assert!(
		compare_strict_q5(&[q5_batch(&[("BRAZIL", 500), ("ARGENTINA", 500)])], &oracle).is_ok(),
		"tie order [BRAZIL, ARGENTINA] equally accepted"
	);
}

#[test]
fn q5_order_prefix_violation_is_fail() {
	let oracle = q5_oracle(&[("ARGENTINA", 100), ("BRAZIL", 200)]);
	assert!(
		compare_strict_q5(&[q5_batch(&[("ARGENTINA", 100), ("BRAZIL", 200)])], &oracle).is_err(),
		"revenue ascending violates ORDER BY revenue DESC"
	);
}

#[test]
fn q5_correctness_mismatch_is_fail_at_compare() {
	let oracle = q5_oracle(&[("ARGENTINA", 500), ("BRAZIL", 300)]);
	assert!(
		compare_strict_q5(&[q5_batch(&[("ARGENTINA", 501), ("BRAZIL", 300)])], &oracle).is_err(),
		"an off-by-one revenue is a mismatch"
	);
}

#[test]
fn q5_missing_or_duplicate_group_is_fail_at_compare() {
	let oracle = q5_oracle(&[("ARGENTINA", 500), ("BRAZIL", 300)]);
	assert!(
		compare_strict_q5(&[q5_batch(&[("ARGENTINA", 500)])], &oracle).is_err(),
		"a dropped group is a mismatch"
	);
	assert!(
		compare_strict_q5(
			&[q5_batch(&[("ARGENTINA", 500), ("ARGENTINA", 500)])],
			&oracle
		)
		.is_err(),
		"a duplicate emitted n_name is rejected"
	);
}

#[test]
fn q5_null_cell_is_fail_at_compare() {
	let oracle = q5_oracle(&[("ARGENTINA", 500)]);
	assert!(
		compare_strict_q5(&[q5_nullable_batch(&[(None, Some(500))])], &oracle).is_err(),
		"a NULL n_name is fail@compare"
	);
	assert!(
		compare_strict_q5(&[q5_nullable_batch(&[(Some("ARGENTINA"), None)])], &oracle).is_err(),
		"a NULL revenue is fail@compare"
	);
}

type Q07NullableRow<'a> = (Option<&'a str>, Option<&'a str>, Option<i32>, Option<i128>);

fn q07_batch(rows: &[(&str, &str, i32, i128)]) -> RecordBatch {
	let nullable_rows: Vec<Q07NullableRow<'_>> = rows
		.iter()
		.map(|&(supp, cust, year, revenue)| (Some(supp), Some(cust), Some(year), Some(revenue)))
		.collect();
	q07_nullable_batch(&nullable_rows)
}

fn q07_nullable_batch(rows: &[Q07NullableRow<'_>]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("supp_nation", DataType::Utf8, true),
		Field::new("cust_nation", DataType::Utf8, true),
		Field::new("l_year", DataType::Int32, true),
		Field::new("revenue", DataType::Decimal128(32, 4), true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.2).collect::<Vec<Option<i32>>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.3).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(32, 4)
					.expect("q07 revenue decimal column"),
			),
		],
	)
	.expect("q07 batch")
}

fn q07_oracle(rows: &[(&str, &str, i32, i128)]) -> Q07Oracle {
	Q07Oracle {
		groups: rows
			.iter()
			.map(|&(supp, cust, year, revenue)| {
				((supp.to_string(), cust.to_string(), year), revenue)
			})
			.collect(),
		selected_rows: rows.len(),
	}
}

#[test]
fn strict_q07_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q07(0.01))
		})
		.expect("spawn q07 correctness thread")
		.join()
		.expect("q07 correctness thread panicked");
	assert_eq!(attempt.query, "q07");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q07_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q07_oracle(0.01);
	assert_eq!(oracle.selected_rows, 46, "{oracle:?}");
	assert_eq!(oracle.groups.len(), 4, "{oracle:?}");
	for (supp, cust, year, revenue) in [
		("FRANCE", "GERMANY", 1995, 2_680_685_774_i128),
		("FRANCE", "GERMANY", 1996, 3_038_622_980),
		("GERMANY", "FRANCE", 1995, 6_211_594_882),
		("GERMANY", "FRANCE", 1996, 3_790_958_854),
	] {
		let key = (supp.to_string(), cust.to_string(), year);
		assert_eq!(
			oracle.groups.get(&key),
			Some(&revenue),
			"q07 group {key:?}: {oracle:?}"
		);
	}
}

#[test]
fn strict_q07_output_schema_is_asserted() {
	assert!(assert_strict_q07_schema(&[q07_batch(&[("FRANCE", "GERMANY", 1995, 100)])]).is_ok());

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("supp_nation", DataType::Utf8, false),
			Field::new("cust_nation", DataType::Utf8, false),
			Field::new("l_year", DataType::Int64, false),
			Field::new("revenue", DataType::Decimal128(32, 4), false),
		])),
		vec![
			Arc::new(StringArray::from(vec!["FRANCE"])),
			Arc::new(StringArray::from(vec!["GERMANY"])),
			Arc::new(Int64Array::from(vec![1995_i64])),
			Arc::new(
				Decimal128Array::from(vec![100_i128])
					.with_precision_and_scale(32, 4)
					.expect("revenue"),
			),
		],
	)
	.expect("drifted q07 batch");
	assert!(
		assert_strict_q07_schema(&[drifted]).is_err(),
		"a non-measured l_year type must trip the schema gate"
	);
}

#[test]
fn q07_supp_cust_nation_not_swapped() {
	let oracle = q07_oracle(&[("FRANCE", "GERMANY", 1995, 100)]);
	assert!(
		compare_strict_q07(&[q07_batch(&[("FRANCE", "GERMANY", 1995, 100)])], &oracle).is_ok(),
		"exact nation direction matches"
	);
	assert!(
		compare_strict_q07(&[q07_batch(&[("GERMANY", "FRANCE", 1995, 100)])], &oracle).is_err(),
		"supplier/customer nation swap must fail"
	);
}

#[test]
fn q07_between_is_inclusive_upper_bound() {
	let lower = q07_contribution("FRANCE", "GERMANY", 9131, 100, 5).expect("lower bound in");
	assert_eq!(lower.0, ("FRANCE".to_string(), "GERMANY".to_string(), 1995));
	assert_eq!(lower.1, 9_500);

	let upper = q07_contribution("FRANCE", "GERMANY", 9861, 100, 5).expect("upper bound in");
	assert_eq!(upper.0, ("FRANCE".to_string(), "GERMANY".to_string(), 1996));

	assert!(
		q07_contribution("FRANCE", "GERMANY", 9130, 100, 5).is_none(),
		"1994-12-31 is outside q07"
	);
	assert!(
		q07_contribution("FRANCE", "GERMANY", 9862, 100, 5).is_none(),
		"1997-01-01 is outside q07"
	);
}

#[test]
fn q07_disjunctive_pair_filter_excludes_other_pairs() {
	assert!(q07_contribution("FRANCE", "GERMANY", 9131, 100, 5).is_some());
	assert!(q07_contribution("GERMANY", "FRANCE", 9131, 100, 5).is_some());
	assert!(
		q07_contribution("FRANCE", "BRAZIL", 9131, 100, 5).is_none(),
		"only the two ordered FR/GER pairs are admitted"
	);
}

#[test]
fn q07_correctness_mismatch_is_fail_at_compare() {
	let oracle = q07_oracle(&[
		("FRANCE", "GERMANY", 1995, 100),
		("GERMANY", "FRANCE", 1996, 200),
	]);
	assert!(
		compare_strict_q07(
			&[q07_batch(&[
				("FRANCE", "GERMANY", 1995, 101),
				("GERMANY", "FRANCE", 1996, 200)
			])],
			&oracle
		)
		.is_err(),
		"an off-by-one revenue is a mismatch"
	);
	assert!(
		compare_strict_q07(
			&[q07_batch(&[
				("FRANCE", "GERMANY", 1996, 100),
				("GERMANY", "FRANCE", 1996, 200)
			])],
			&oracle
		)
		.is_err(),
		"a wrong l_year changes the group key"
	);
}

#[test]
fn q07_missing_extra_or_duplicate_group_is_fail() {
	let oracle = q07_oracle(&[
		("FRANCE", "GERMANY", 1995, 100),
		("GERMANY", "FRANCE", 1996, 200),
	]);
	assert!(
		compare_strict_q07(&[q07_batch(&[("FRANCE", "GERMANY", 1995, 100)])], &oracle).is_err(),
		"a dropped group is a mismatch"
	);
	assert!(
		compare_strict_q07(
			&[q07_batch(&[
				("FRANCE", "GERMANY", 1995, 100),
				("GERMANY", "FRANCE", 1996, 200),
				("FRANCE", "GERMANY", 1996, 300)
			])],
			&oracle
		)
		.is_err(),
		"an extra group is a mismatch"
	);
	assert!(
		compare_strict_q07(
			&[q07_batch(&[
				("FRANCE", "GERMANY", 1995, 100),
				("FRANCE", "GERMANY", 1995, 100)
			])],
			&oracle
		)
		.is_err(),
		"a duplicate emitted group is rejected"
	);
}

#[test]
fn q07_null_cell_is_fail() {
	let oracle = q07_oracle(&[("FRANCE", "GERMANY", 1995, 100)]);
	let rows: [Q07NullableRow<'_>; 4] = [
		(None, Some("GERMANY"), Some(1995), Some(100)),
		(Some("FRANCE"), None, Some(1995), Some(100)),
		(Some("FRANCE"), Some("GERMANY"), None, Some(100)),
		(Some("FRANCE"), Some("GERMANY"), Some(1995), None),
	];
	for row in rows {
		assert!(
			compare_strict_q07(&[q07_nullable_batch(&[row])], &oracle).is_err(),
			"NULL cell is fail@compare"
		);
	}
}

#[test]
fn q07_order_violation_is_fail() {
	let oracle = q07_oracle(&[
		("FRANCE", "GERMANY", 1995, 100),
		("GERMANY", "FRANCE", 1996, 200),
	]);
	assert!(
		compare_strict_q07(
			&[q07_batch(&[
				("GERMANY", "FRANCE", 1996, 200),
				("FRANCE", "GERMANY", 1995, 100)
			])],
			&oracle
		)
		.is_err(),
		"q07's ORDER BY supp_nation, cust_nation, l_year is part of the exact answer"
	);
}

type Q08NullableRow = (Option<i32>, Option<i128>);

fn q08_batch(rows: &[(i32, i128)]) -> RecordBatch {
	let nullable_rows: Vec<Q08NullableRow> = rows
		.iter()
		.map(|&(year, share)| (Some(year), Some(share)))
		.collect();
	q08_nullable_batch(&nullable_rows)
}

fn q08_nullable_batch(rows: &[Q08NullableRow]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("o_year", DataType::Int32, true),
		Field::new("mkt_share", DataType::Decimal128(38, 8), true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<i32>>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.1).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(38, 8)
					.expect("q08 share decimal column"),
			),
		],
	)
	.expect("q08 batch")
}

fn q08_oracle(rows: &[(i32, i128)]) -> Q08Oracle {
	Q08Oracle {
		groups: rows
			.iter()
			.map(|&(year, share)| {
				(
					year,
					DecimalRaw {
						value: Some(share),
						precision: 38,
						scale: 8,
					},
				)
			})
			.collect(),
		denominator_rows: rows.len(),
		brazil_rows: 1,
	}
}

#[test]
fn strict_q08_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q08(0.01))
		})
		.expect("spawn q08 correctness thread")
		.join()
		.expect("q08 correctness thread panicked");
	assert_eq!(attempt.query, "q08");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q08_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q08_oracle(0.01).expect("q08 oracle");
	assert_eq!(oracle.groups.len(), 2, "{oracle:?}");
	assert!(oracle.groups.contains_key(&1995), "{oracle:?}");
	assert!(oracle.groups.contains_key(&1996), "{oracle:?}");
	assert!(
		oracle.denominator_rows > 0,
		"denominator rows: {}",
		oracle.denominator_rows
	);
	// The live data may or may not exercise the BRAZIL branch; the results doc records the measured value.
	assert!(oracle.brazil_rows <= oracle.denominator_rows, "{oracle:?}");
	for (year, share) in &oracle.groups {
		assert_eq!(share.precision, 38, "year {year}: {share:?}");
		assert_eq!(share.scale, 8, "year {year}: {share:?}");
		assert!(share.value.is_some(), "year {year}: {share:?}");
	}
}

#[test]
fn strict_q08_output_schema_is_asserted() {
	assert!(assert_strict_q08_schema(&[q08_batch(&[(1995, 100)])]).is_ok());
	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("o_year", DataType::Int64, false),
			Field::new("mkt_share", DataType::Decimal128(38, 8), false),
		])),
		vec![
			Arc::new(Int64Array::from(vec![1995_i64])),
			Arc::new(
				Decimal128Array::from(vec![100_i128])
					.with_precision_and_scale(38, 8)
					.expect("share"),
			),
		],
	)
	.expect("drifted q08 batch");
	assert!(
		assert_strict_q08_schema(&[drifted]).is_err(),
		"a non-measured o_year type must trip the schema gate"
	);
}

#[test]
fn q08_ratio_uses_engine_decimal_division() {
	let rounded = q08_market_share_raw(1, 6).expect("q08 ratio");
	assert_eq!(rounded.precision, 38);
	assert_eq!(rounded.scale, 8);
	assert_eq!(
		rounded.value,
		Some(16_666_667),
		"1/6 at output scale 8 rounds half away, not truncates"
	);
}

#[test]
fn q08_self_join_customer_vs_supplier_not_conflated() {
	assert!(
		q08_contribution("ECONOMY ANODIZED STEEL", "AMERICA", "BRAZIL", 9131, 100, 5).is_some(),
		"AMERICA customer + BRAZIL supplier is the numerator branch"
	);
	let (_, _, is_brazil) = q08_contribution(
		"ECONOMY ANODIZED STEEL",
		"AMERICA",
		"ARGENTINA",
		9131,
		100,
		5,
	)
	.expect("AMERICA customer with non-BRAZIL supplier contributes denominator");
	assert!(!is_brazil, "non-BRAZIL supplier is denominator-only");
	assert!(
		q08_contribution("ECONOMY ANODIZED STEEL", "ASIA", "BRAZIL", 9131, 100, 5).is_none(),
		"supplier BRAZIL does not satisfy the customer-region AMERICA filter"
	);
}

#[test]
fn q08_filters_and_date_bounds_are_exact() {
	assert!(
		q08_contribution("ECONOMY ANODIZED STEEL", "AMERICA", "BRAZIL", 9131, 100, 5).is_some(),
		"1995-01-01 lower bound included"
	);
	assert!(
		q08_contribution("ECONOMY ANODIZED STEEL", "AMERICA", "BRAZIL", 9861, 100, 5).is_some(),
		"1996-12-31 upper bound included"
	);
	assert!(
		q08_contribution("ECONOMY ANODIZED STEEL", "AMERICA", "BRAZIL", 9862, 100, 5).is_none(),
		"1997-01-01 excluded"
	);
	assert!(
		q08_contribution("ECONOMY ANODIZED BRASS", "AMERICA", "BRAZIL", 9131, 100, 5).is_none(),
		"part type must match exactly"
	);
}

#[test]
fn q08_brazil_numerator_group() {
	let (_, brazil_volume, is_brazil) =
		q08_contribution("ECONOMY ANODIZED STEEL", "AMERICA", "BRAZIL", 9131, 100, 5)
			.expect("BRAZIL row contributes");
	assert!(is_brazil);
	let (_, denominator_only, is_brazil) = q08_contribution(
		"ECONOMY ANODIZED STEEL",
		"AMERICA",
		"ARGENTINA",
		9131,
		100,
		5,
	)
	.expect("non-BRAZIL row contributes");
	assert!(!is_brazil);
	let share =
		q08_market_share_raw(brazil_volume, brazil_volume + denominator_only).expect("ratio");
	assert_eq!(
		share.value,
		Some(50_000_000),
		"one BRAZIL row over two equal rows gives 0.50000000"
	);
}

#[test]
fn q08_correctness_mismatch_is_fail_at_compare() {
	let oracle = q08_oracle(&[(1995, 100), (1996, 200)]);
	assert!(
		compare_strict_q08(&[q08_batch(&[(1995, 101), (1996, 200)])], &oracle).is_err(),
		"an off-by-one mkt_share is a mismatch"
	);
	assert!(
		compare_strict_q08(&[q08_batch(&[(1994, 100), (1996, 200)])], &oracle).is_err(),
		"a wrong o_year changes the group key"
	);
}

#[test]
fn q08_missing_extra_or_duplicate_group_is_fail() {
	let oracle = q08_oracle(&[(1995, 100), (1996, 200)]);
	assert!(
		compare_strict_q08(&[q08_batch(&[(1995, 100)])], &oracle).is_err(),
		"a dropped year is a mismatch"
	);
	assert!(
		compare_strict_q08(
			&[q08_batch(&[(1995, 100), (1996, 200), (1997, 300)])],
			&oracle
		)
		.is_err(),
		"an extra year is a mismatch"
	);
	assert!(
		compare_strict_q08(&[q08_batch(&[(1995, 100), (1995, 100)])], &oracle).is_err(),
		"a duplicate emitted year is rejected"
	);
}

#[test]
fn q08_null_cell_is_fail() {
	let oracle = q08_oracle(&[(1995, 100), (1996, 200)]);
	assert!(
		compare_strict_q08(
			&[q08_nullable_batch(&[
				(None, Some(100)),
				(Some(1996), Some(200))
			])],
			&oracle
		)
		.is_err(),
		"a NULL o_year is fail@compare"
	);
	assert!(
		compare_strict_q08(
			&[q08_nullable_batch(&[
				(Some(1995), None),
				(Some(1996), Some(200))
			])],
			&oracle
		)
		.is_err(),
		"a NULL mkt_share is fail@compare"
	);
}

#[test]
fn q08_order_violation_is_fail() {
	let oracle = q08_oracle(&[(1995, 100), (1996, 200)]);
	assert!(
		compare_strict_q08(&[q08_batch(&[(1996, 200), (1995, 100)])], &oracle).is_err(),
		"q08's ORDER BY o_year is part of the exact answer"
	);
}

type Q09NullableRow<'a> = (Option<&'a str>, Option<i32>, Option<i128>);

fn q09_batch(rows: &[(&str, i32, i128)]) -> RecordBatch {
	let nullable_rows: Vec<Q09NullableRow<'_>> = rows
		.iter()
		.map(|&(nation, year, profit)| (Some(nation), Some(year), Some(profit)))
		.collect();
	q09_nullable_batch(&nullable_rows)
}

fn q09_nullable_batch(rows: &[Q09NullableRow<'_>]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("nation", DataType::Utf8, true),
		Field::new("o_year", DataType::Int32, true),
		Field::new("sum_profit", DataType::Decimal128(33, 4), true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(Int32Array::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<i32>>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.2).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(33, 4)
					.expect("q09 profit decimal column"),
			),
		],
	)
	.expect("q09 batch")
}

fn q09_oracle(rows: &[(&str, i32, i128)]) -> Q09Oracle {
	Q09Oracle {
		groups: rows
			.iter()
			.map(|&(nation, year, profit)| ((nation.to_string(), year), profit))
			.collect(),
		selected_rows: rows.len(),
		negative_amount_rows: rows.iter().filter(|row| row.2 < 0).count(),
	}
}

#[test]
fn strict_q09_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q09(0.01))
		})
		.expect("spawn q09 correctness thread")
		.join()
		.expect("q09 correctness thread panicked");
	assert_eq!(attempt.query, "q09");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q09_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q09_oracle(0.01);
	assert!(
		oracle.selected_rows > 0,
		"selected q09 rows at sf=0.01: {}",
		oracle.selected_rows
	);
	assert!(
		oracle.groups.len() > 1,
		"q09 groups at sf=0.01: {}",
		oracle.groups.len()
	);
	assert!(
		oracle.negative_amount_rows <= oracle.selected_rows,
		"negative rows cannot exceed selected rows: {oracle:?}"
	);
}

#[test]
fn strict_q09_output_schema_is_asserted() {
	assert!(
		assert_strict_q09_schema(&[q09_batch(&[
			("ARGENTINA", 1995, 100),
			("BRAZIL", 1994, 200)
		])])
		.is_ok()
	);

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("nation", DataType::Utf8, false),
			Field::new("o_year", DataType::Int64, false),
			Field::new("sum_profit", DataType::Decimal128(33, 4), false),
		])),
		vec![
			Arc::new(StringArray::from(vec!["ARGENTINA"])) as ArrayRef,
			Arc::new(Int64Array::from(vec![1995_i64])) as ArrayRef,
			Arc::new(
				Decimal128Array::from(vec![100_i128])
					.with_precision_and_scale(33, 4)
					.expect("profit"),
			) as ArrayRef,
		],
	)
	.expect("drifted q09 batch");
	assert!(
		assert_strict_q09_schema(&[drifted]).is_err(),
		"a non-measured o_year type must trip the schema gate"
	);
}

#[test]
fn q09_amount_is_revenue_minus_cost_signed() {
	let amount = q09_amount_raw(10_000, 0, 15_000, 100);
	assert_eq!(amount, -500_000, "100.00*(1-0.00)-150.00*1.00 = -50.0000");
	assert_ne!(
		amount,
		q09_amount_raw(10_000, 0, 0, 100),
		"dropping the cost term must change the answer"
	);
}

#[test]
fn q09_partsupp_composite_key_picks_right_cost() {
	let costs = HashMap::from([((1_i64, 10_i64), 100_i128), ((1_i64, 11_i64), 200_i128)]);
	assert_eq!(q09_partsupp_cost(&costs, 1, 10), Some(100));
	assert_eq!(q09_partsupp_cost(&costs, 1, 11), Some(200));
	assert_eq!(
		q09_partsupp_cost(&costs, 2, 10),
		None,
		"both partkey and suppkey are required"
	);
}

#[test]
fn q09_green_is_contains_match() {
	assert!(q09_part_name_contains_green("forest green"));
	assert!(q09_part_name_contains_green("green forest"));
	assert!(
		!q09_part_name_contains_green("forest"),
		"q09 uses contains LIKE, not a broader color inference"
	);
	assert!(
		!q09_part_name_contains_green("GREEN"),
		"LIKE matching is case-sensitive in the current harness"
	);
}

#[test]
fn q09_order_year_extracts_general_date32_years() {
	assert_eq!(q09_order_year(0), 1970);
	assert_eq!(q09_order_year(9131), 1995);
	assert_eq!(q09_order_year(9861), 1996);
	assert_eq!(q09_order_year(9862), 1997);
}

#[test]
fn q09_correctness_mismatch_is_fail_at_compare() {
	let oracle = q09_oracle(&[("ARGENTINA", 1995, 100), ("BRAZIL", 1994, 200)]);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("ARGENTINA", 1995, 101),
				("BRAZIL", 1994, 200)
			])],
			&oracle
		)
		.is_err(),
		"an off-by-one sum_profit is a mismatch"
	);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("ARGENTINA", 1996, 100),
				("BRAZIL", 1994, 200)
			])],
			&oracle
		)
		.is_err(),
		"a wrong o_year changes the group key"
	);
}

#[test]
fn q09_missing_extra_or_duplicate_group_is_fail() {
	let oracle = q09_oracle(&[("ARGENTINA", 1995, 100), ("BRAZIL", 1994, 200)]);
	assert!(
		compare_strict_q09(&[q09_batch(&[("ARGENTINA", 1995, 100)])], &oracle).is_err(),
		"a dropped group is a mismatch"
	);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("ARGENTINA", 1995, 100),
				("BRAZIL", 1994, 200),
				("CANADA", 1993, 300)
			])],
			&oracle
		)
		.is_err(),
		"an extra group is a mismatch"
	);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("ARGENTINA", 1995, 100),
				("ARGENTINA", 1995, 100)
			])],
			&oracle
		)
		.is_err(),
		"a duplicate emitted group is rejected"
	);
}

#[test]
fn q09_null_cell_is_fail() {
	let oracle = q09_oracle(&[("ARGENTINA", 1995, 100), ("BRAZIL", 1994, 200)]);
	for row in [
		(None, Some(1995), Some(100)),
		(Some("ARGENTINA"), None, Some(100)),
		(Some("ARGENTINA"), Some(1995), None),
	] {
		assert!(
			compare_strict_q09(
				&[q09_nullable_batch(&[
					row,
					(Some("BRAZIL"), Some(1994), Some(200))
				])],
				&oracle
			)
			.is_err(),
			"NULL cell is fail@compare"
		);
	}
}

#[test]
fn q09_order_violation_is_fail() {
	let oracle = q09_oracle(&[("ARGENTINA", 1995, 100), ("ARGENTINA", 1994, 200)]);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("ARGENTINA", 1994, 200),
				("ARGENTINA", 1995, 100)
			])],
			&oracle
		)
		.is_err(),
		"q09's ORDER BY nation ASC, o_year DESC is part of the exact answer"
	);
	let oracle = q09_oracle(&[("ARGENTINA", 1995, 100), ("BRAZIL", 1995, 200)]);
	assert!(
		compare_strict_q09(
			&[q09_batch(&[
				("BRAZIL", 1995, 200),
				("ARGENTINA", 1995, 100)
			])],
			&oracle
		)
		.is_err(),
		"nation order is ascending"
	);
}

fn q10_row(c_custkey: i64, revenue: i128) -> Q10Row {
	Q10Row {
		c_custkey,
		c_name: format!("Customer#{c_custkey:09}"),
		revenue,
		c_acctbal: 10_000 + c_custkey as i128,
		n_name: "ARGENTINA".to_string(),
		c_address: format!("{c_custkey} Main Street"),
		c_phone: format!("10-000-000-{c_custkey:04}"),
		c_comment: format!("comment {c_custkey}"),
	}
}

fn q10_oracle(rows: &[Q10Row]) -> Q10Oracle {
	Q10Oracle {
		rows: rows.to_vec(),
		selected_rows: rows.len(),
	}
}

fn q10_batch(rows: &[Q10Row]) -> RecordBatch {
	let nullable_rows: Vec<Q10NullableRow<'_>> = rows
		.iter()
		.map(|row| {
			(
				Some(row.c_custkey),
				Some(row.c_name.as_str()),
				Some(row.revenue),
				Some(row.c_acctbal),
				Some(row.n_name.as_str()),
				Some(row.c_address.as_str()),
				Some(row.c_phone.as_str()),
				Some(row.c_comment.as_str()),
			)
		})
		.collect();
	q10_nullable_batch(&nullable_rows)
}

type Q10NullableRow<'a> = (
	Option<i64>,
	Option<&'a str>,
	Option<i128>,
	Option<i128>,
	Option<&'a str>,
	Option<&'a str>,
	Option<&'a str>,
	Option<&'a str>,
);

fn q10_nullable_batch(rows: &[Q10NullableRow<'_>]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("c_custkey", DataType::Int64, true),
		Field::new("c_name", DataType::Utf8, true),
		Field::new("revenue", DataType::Decimal128(32, 4), true),
		Field::new("c_acctbal", DataType::Decimal128(15, 2), true),
		Field::new("n_name", DataType::Utf8, true),
		Field::new("c_address", DataType::Utf8, true),
		Field::new("c_phone", DataType::Utf8, true),
		Field::new("c_comment", DataType::Utf8, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<i64>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.2).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(32, 4)
					.expect("q10 revenue decimal column"),
			),
			Arc::new(
				Decimal128Array::from(rows.iter().map(|row| row.3).collect::<Vec<Option<i128>>>())
					.with_precision_and_scale(15, 2)
					.expect("q10 c_acctbal decimal column"),
			),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.4).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.5).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.6).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.7).collect::<Vec<Option<&str>>>(),
			)),
		],
	)
	.expect("q10 batch")
}

fn q10_batch_with_null(null_column: usize) -> RecordBatch {
	let row: Q10NullableRow<'_> = (
		(null_column != 0).then_some(1),
		(null_column != 1).then_some("Customer#000000001"),
		(null_column != 2).then_some(500),
		(null_column != 3).then_some(12345),
		(null_column != 4).then_some("ARGENTINA"),
		(null_column != 5).then_some("1 Main Street"),
		(null_column != 6).then_some("10-000-000-0001"),
		(null_column != 7).then_some("comment 1"),
	);
	q10_nullable_batch(&[row])
}

#[test]
fn strict_q10_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q10(0.01))
		})
		.expect("spawn q10 correctness thread")
		.join()
		.expect("q10 correctness thread panicked");
	assert_eq!(attempt.query, "q10");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q10_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q10_oracle(0.01);
	assert!(
		oracle.selected_rows > 1,
		"selected {} contributing rows",
		oracle.selected_rows
	);
	assert!(
		oracle.rows.len() > 1,
		"expected > 1 q10 group, got {}",
		oracle.rows.len()
	);
}

#[test]
fn strict_q10_output_schema_is_asserted() {
	let row = q10_row(1, 500);
	assert!(assert_strict_q10_schema(&[q10_batch(&[row])]).is_ok());

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("c_custkey", DataType::Int64, false),
			Field::new("c_name", DataType::Utf8, false),
			Field::new("revenue", DataType::Decimal128(31, 4), false),
			Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
			Field::new("n_name", DataType::Utf8, false),
			Field::new("c_address", DataType::Utf8, false),
			Field::new("c_phone", DataType::Utf8, false),
			Field::new("c_comment", DataType::Utf8, false),
		])),
		vec![
			Arc::new(Int64Array::from(vec![1])),
			Arc::new(StringArray::from(vec!["Customer#000000001"])),
			Arc::new(
				Decimal128Array::from(vec![500_i128])
					.with_precision_and_scale(31, 4)
					.expect("drifted revenue"),
			),
			Arc::new(
				Decimal128Array::from(vec![12345_i128])
					.with_precision_and_scale(15, 2)
					.expect("acctbal"),
			),
			Arc::new(StringArray::from(vec!["ARGENTINA"])),
			Arc::new(StringArray::from(vec!["1 Main Street"])),
			Arc::new(StringArray::from(vec!["10-000-000-0001"])),
			Arc::new(StringArray::from(vec!["comment 1"])),
		],
	)
	.expect("drifted q10 batch");
	assert!(
		assert_strict_q10_schema(&[drifted]).is_err(),
		"a non-measured revenue precision must trip the schema gate"
	);
}

#[test]
fn q10_correctness_mismatch_is_fail_at_compare() {
	let row = q10_row(1, 500);
	let oracle = q10_oracle(std::slice::from_ref(&row));
	assert!(
		compare_strict_q10(&[q10_batch(std::slice::from_ref(&row))], &oracle).is_ok(),
		"exact row match"
	);

	let mut wrong_revenue = row.clone();
	wrong_revenue.revenue += 1;
	assert!(
		compare_strict_q10(&[q10_batch(&[wrong_revenue])], &oracle).is_err(),
		"an off-by-one revenue is a mismatch"
	);

	let mut wrong_acctbal = row.clone();
	wrong_acctbal.c_acctbal += 1;
	assert!(
		compare_strict_q10(&[q10_batch(&[wrong_acctbal])], &oracle).is_err(),
		"an off-by-one c_acctbal is a mismatch"
	);

	let mut wrong_name = row;
	wrong_name.n_name = "BRAZIL".to_string();
	assert!(
		compare_strict_q10(&[q10_batch(&[wrong_name])], &oracle).is_err(),
		"a wrong string attribute is a mismatch"
	);
}

#[test]
fn q10_missing_extra_or_duplicate_row_is_fail_at_compare() {
	let row_a = q10_row(1, 500);
	let row_b = q10_row(2, 300);
	let oracle = q10_oracle(&[row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q10(&[q10_batch(std::slice::from_ref(&row_a))], &oracle).is_err(),
		"a dropped row is a mismatch"
	);
	let row_c = q10_row(3, 100);
	assert!(
		compare_strict_q10(
			&[q10_batch(&[row_a.clone(), row_b.clone(), row_c])],
			&oracle
		)
		.is_err(),
		"an extra row is a mismatch"
	);
	assert!(
		compare_strict_q10(&[q10_batch(&[row_a.clone(), row_a])], &oracle).is_err(),
		"a duplicate emitted c_custkey is rejected"
	);
}

#[test]
fn q10_null_cell_is_fail_at_compare() {
	let oracle = q10_oracle(&[q10_row(1, 500)]);
	for column in 0..8 {
		assert!(
			compare_strict_q10(&[q10_batch_with_null(column)], &oracle).is_err(),
			"NULL in q10 column {column} is fail@compare"
		);
	}
}

#[test]
fn q10_order_prefix_violation_is_fail() {
	let row_a = q10_row(1, 500);
	let row_b = q10_row(2, 300);
	let oracle = q10_oracle(&[row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q10(&[q10_batch(&[row_b, row_a])], &oracle).is_err(),
		"revenue ascending violates ORDER BY revenue DESC"
	);
}

#[test]
fn q10_comparator_accepts_tied_revenue_permutation() {
	let row_a = q10_row(1, 500);
	let row_b = q10_row(2, 500);
	let oracle = q10_oracle(&[row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q10(&[q10_batch(&[row_a.clone(), row_b.clone()])], &oracle).is_ok(),
		"tie order [a,b] accepted"
	);
	assert!(
		compare_strict_q10(&[q10_batch(&[row_b, row_a])], &oracle).is_ok(),
		"tie order [b,a] equally accepted"
	);
}

fn q04_batch(rows: &[(&str, i64)]) -> RecordBatch {
	let nullable_rows: Vec<(Option<&str>, Option<i64>)> = rows
		.iter()
		.map(|&(priority, count)| (Some(priority), Some(count)))
		.collect();
	q04_nullable_batch(&nullable_rows)
}

fn q04_nullable_batch(rows: &[(Option<&str>, Option<i64>)]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("o_orderpriority", DataType::Utf8, true),
		Field::new("order_count", DataType::Int64, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<i64>>>(),
			)),
		],
	)
	.expect("q04 batch")
}

fn q04_int32_count_batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("o_orderpriority", DataType::Utf8, true),
		Field::new("order_count", DataType::Int32, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["1-URGENT"])),
			Arc::new(Int32Array::from(vec![1])),
		],
	)
	.expect("q04 int32-count batch")
}

fn q04_oracle(rows: &[(&str, i64)]) -> Q04Oracle {
	Q04Oracle {
		groups: rows
			.iter()
			.map(|&(priority, count)| (priority.to_string(), count))
			.collect(),
		orders_pass_exists: rows.iter().map(|(_, count)| *count).sum::<i64>() as usize,
		orders_fail_exists: 1,
	}
}

#[tokio::test]
async fn strict_q04_matches_independent_oracle() {
	let attempt = check_strict_q04(0.01).await;
	assert_eq!(attempt.query, "q04");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q04_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q04_oracle(0.01);
	assert_eq!(oracle.orders_pass_exists, 535, "{oracle:?}");
	assert_eq!(oracle.orders_fail_exists, 47, "{oracle:?}");
	for priority in ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"] {
		let count = oracle
			.groups
			.get(priority)
			.unwrap_or_else(|| panic!("missing priority group {priority}: {oracle:?}"));
		assert!(*count > 0, "{priority} count: {count}");
	}
}

#[test]
fn strict_q04_output_schema_is_asserted() {
	assert!(assert_strict_q04_schema(&[q04_batch(&[("1-URGENT", 1)])]).is_ok());
	assert!(
		assert_strict_q04_schema(&[q04_int32_count_batch()]).is_err(),
		"a non-measured count type must trip the schema gate"
	);
}

#[test]
fn q04_exists_counts_order_once() {
	let oracle = q04_oracle_from_rows(
		[(1_i64, 8582_i32, "1-URGENT".to_string())],
		[(1_i64, 1_i32, 2_i32), (1, 2, 3)],
	);
	assert_eq!(oracle.orders_pass_exists, 1, "{oracle:?}");
	assert_eq!(oracle.orders_fail_exists, 0, "{oracle:?}");
	assert_eq!(
		oracle.groups.get("1-URGENT"),
		Some(&1),
		"multiple qualifying lineitems still count one order"
	);
}

#[test]
fn q04_order_failing_exists_is_excluded() {
	let oracle = q04_oracle_from_rows(
		[
			(1_i64, 8582_i32, "1-URGENT".to_string()),
			(2_i64, 8582_i32, "2-HIGH".to_string()),
			(3_i64, 8582_i32, "3-MEDIUM".to_string()),
			(4_i64, 8674_i32, "4-NOT SPECIFIED".to_string()),
		],
		[(1_i64, 1_i32, 2_i32), (2, 3, 3), (4, 1, 2)],
	);
	assert_eq!(oracle.orders_pass_exists, 1, "{oracle:?}");
	assert_eq!(oracle.orders_fail_exists, 2, "{oracle:?}");
	assert_eq!(oracle.groups.get("1-URGENT"), Some(&1));
	assert!(
		!oracle.groups.contains_key("2-HIGH"),
		"commitdate >= receiptdate fails EXISTS"
	);
	assert!(
		!oracle.groups.contains_key("3-MEDIUM"),
		"no lineitem fails EXISTS"
	);
	assert!(
		!oracle.groups.contains_key("4-NOT SPECIFIED"),
		"orders outside the q04 date window are ignored"
	);
}

#[test]
fn q04_correctness_mismatch_is_fail_at_compare() {
	let oracle = q04_oracle(&[("1-URGENT", 3), ("2-HIGH", 4)]);
	assert!(
		compare_strict_q04(&[q04_batch(&[("1-URGENT", 3), ("2-HIGH", 4)])], &oracle).is_ok(),
		"exact order counts match"
	);
	assert!(
		compare_strict_q04(&[q04_batch(&[("1-URGENT", 4), ("2-HIGH", 4)])], &oracle).is_err(),
		"an off-by-one order_count is a mismatch"
	);
}

#[test]
fn q04_missing_extra_or_duplicate_group_is_fail_at_compare() {
	let oracle = q04_oracle(&[("1-URGENT", 3), ("2-HIGH", 4)]);
	assert!(
		compare_strict_q04(&[q04_batch(&[("1-URGENT", 3)])], &oracle).is_err(),
		"a dropped priority group is a mismatch"
	);
	assert!(
		compare_strict_q04(
			&[q04_batch(&[
				("1-URGENT", 3),
				("2-HIGH", 4),
				("3-MEDIUM", 5)
			])],
			&oracle
		)
		.is_err(),
		"an extra priority group is a mismatch"
	);
	assert!(
		compare_strict_q04(&[q04_batch(&[("1-URGENT", 3), ("1-URGENT", 3)])], &oracle).is_err(),
		"a duplicate emitted priority group is rejected"
	);
}

#[test]
fn q04_null_cell_is_fail_at_compare() {
	let oracle = q04_oracle(&[("1-URGENT", 3), ("2-HIGH", 4)]);
	assert!(
		compare_strict_q04(
			&[q04_nullable_batch(&[
				(None, Some(3)),
				(Some("2-HIGH"), Some(4))
			])],
			&oracle
		)
		.is_err(),
		"a NULL o_orderpriority is fail@compare"
	);
	assert!(
		compare_strict_q04(
			&[q04_nullable_batch(&[
				(Some("1-URGENT"), None),
				(Some("2-HIGH"), Some(4))
			])],
			&oracle
		)
		.is_err(),
		"a NULL order_count is fail@compare"
	);
}

#[test]
fn q04_order_violation_is_fail_at_compare() {
	let oracle = q04_oracle(&[("1-URGENT", 3), ("2-HIGH", 4)]);
	assert!(
		compare_strict_q04(&[q04_batch(&[("2-HIGH", 4), ("1-URGENT", 3)])], &oracle).is_err(),
		"q04's ORDER BY o_orderpriority is part of the exact answer"
	);
}

fn q12_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
	let nullable_rows: Vec<(Option<&str>, Option<i64>, Option<i64>)> = rows
		.iter()
		.map(|&(shipmode, high, low)| (Some(shipmode), Some(high), Some(low)))
		.collect();
	q12_nullable_batch(&nullable_rows)
}

fn q12_nullable_batch(rows: &[(Option<&str>, Option<i64>, Option<i64>)]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("l_shipmode", DataType::Utf8, true),
		Field::new("high_line_count", DataType::Int64, true),
		Field::new("low_line_count", DataType::Int64, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<i64>>>(),
			)),
			Arc::new(Int64Array::from(
				rows.iter().map(|row| row.2).collect::<Vec<Option<i64>>>(),
			)),
		],
	)
	.expect("q12 batch")
}

fn q12_int32_count_batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("l_shipmode", DataType::Utf8, true),
		Field::new("high_line_count", DataType::Int32, true),
		Field::new("low_line_count", DataType::Int32, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(vec!["MAIL"])),
			Arc::new(Int32Array::from(vec![1])),
			Arc::new(Int32Array::from(vec![2])),
		],
	)
	.expect("q12 int32-count batch")
}

fn q12_oracle(rows: &[(&str, i64, i64)]) -> Q12Oracle {
	let groups: BTreeMap<String, Q12Group> = rows
		.iter()
		.map(|&(shipmode, high, low)| {
			(
				shipmode.to_string(),
				Q12Group {
					high_line_count: high,
					low_line_count: low,
				},
			)
		})
		.collect();
	Q12Oracle {
		groups,
		selected_rows: rows.iter().map(|(_, high, low)| high + low).sum::<i64>() as usize,
		high_rows: rows.iter().map(|(_, high, _)| *high).sum::<i64>() as usize,
		low_rows: rows.iter().map(|(_, _, low)| *low).sum::<i64>() as usize,
	}
}

#[tokio::test]
async fn strict_q12_matches_independent_oracle() {
	let attempt = check_strict_q12(0.01).await;
	assert_eq!(attempt.query, "q12");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q12_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q12_oracle(0.01);
	assert!(oracle.groups.contains_key("MAIL"), "{oracle:?}");
	assert!(oracle.groups.contains_key("SHIP"), "{oracle:?}");
	assert!(
		oracle.selected_rows > 0,
		"selected {} contributing rows",
		oracle.selected_rows
	);
	assert!(oracle.high_rows > 0, "high rows: {}", oracle.high_rows);
	assert!(oracle.low_rows > 0, "low rows: {}", oracle.low_rows);
	for (shipmode, group) in &oracle.groups {
		assert!(
			group.high_line_count > 0,
			"{shipmode} high rows: {}",
			group.high_line_count
		);
		assert!(
			group.low_line_count > 0,
			"{shipmode} low rows: {}",
			group.low_line_count
		);
	}
}

#[test]
fn strict_q12_output_schema_is_asserted() {
	assert!(assert_strict_q12_schema(&[q12_batch(&[("MAIL", 1, 2)])]).is_ok());
	assert!(
		assert_strict_q12_schema(&[q12_int32_count_batch()]).is_err(),
		"a non-measured count type must trip the schema gate"
	);
}

#[test]
fn q12_correctness_mismatch_is_fail_at_compare() {
	let oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 3, 4), ("SHIP", 5, 6)])], &oracle).is_ok(),
		"exact high/low counts match"
	);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 4, 4), ("SHIP", 5, 6)])], &oracle).is_err(),
		"an off-by-one high_line_count is a mismatch"
	);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 3, 5), ("SHIP", 5, 6)])], &oracle).is_err(),
		"an off-by-one low_line_count is a mismatch"
	);
}

#[test]
fn q12_high_low_swapped_is_fail_at_compare() {
	let oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 4, 3), ("SHIP", 5, 6)])], &oracle).is_err(),
		"swapping the CASE arms must fail"
	);
}

#[test]
fn q12_missing_or_duplicate_group_is_fail_at_compare() {
	let oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 3, 4)])], &oracle).is_err(),
		"a dropped shipmode group is a mismatch"
	);
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 3, 4), ("MAIL", 3, 4)])], &oracle).is_err(),
		"a duplicate emitted l_shipmode is rejected"
	);
}

#[test]
fn q12_null_cell_is_fail_at_compare() {
	let oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	assert!(
		compare_strict_q12(
			&[q12_nullable_batch(&[
				(None, Some(3), Some(4)),
				(Some("SHIP"), Some(5), Some(6))
			])],
			&oracle
		)
		.is_err(),
		"a NULL l_shipmode is fail@compare"
	);
	assert!(
		compare_strict_q12(
			&[q12_nullable_batch(&[
				(Some("MAIL"), None, Some(4)),
				(Some("SHIP"), Some(5), Some(6))
			])],
			&oracle
		)
		.is_err(),
		"a NULL high_line_count is fail@compare"
	);
	assert!(
		compare_strict_q12(
			&[q12_nullable_batch(&[
				(Some("MAIL"), Some(3), None),
				(Some("SHIP"), Some(5), Some(6))
			])],
			&oracle
		)
		.is_err(),
		"a NULL low_line_count is fail@compare"
	);
}

#[test]
fn q12_out_of_order_groups_is_fail_at_compare() {
	let oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	assert!(
		compare_strict_q12(&[q12_batch(&[("SHIP", 5, 6), ("MAIL", 3, 4)])], &oracle).is_err(),
		"q12's ORDER BY l_shipmode is part of the exact answer"
	);
}

#[test]
fn q12_hollow_oracle_is_fail_at_compare() {
	let mut oracle = q12_oracle(&[("MAIL", 0, 4), ("SHIP", 5, 6)]);
	oracle.high_rows = 5;
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 0, 4), ("SHIP", 5, 6)])], &oracle).is_err(),
		"a per-group missing CASE branch is a hollow oracle"
	);

	let mut oracle = q12_oracle(&[("MAIL", 3, 4), ("SHIP", 5, 6)]);
	oracle.low_rows = 0;
	assert!(
		compare_strict_q12(&[q12_batch(&[("MAIL", 3, 4), ("SHIP", 5, 6)])], &oracle).is_err(),
		"a globally missing CASE branch is a hollow oracle"
	);
}

fn q14_batch(values: &[Option<i128>], precision: u8, scale: i8) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"promo_revenue",
		DataType::Decimal128(precision, scale),
		true,
	)]));
	RecordBatch::try_new(
		schema,
		vec![Arc::new(
			Decimal128Array::from(values.to_vec())
				.with_precision_and_scale(precision, scale)
				.expect("q14 promo_revenue decimal column"),
		)],
	)
	.expect("q14 batch")
}

fn q14_oracle(value: Option<i128>) -> Q14Oracle {
	Q14Oracle {
		promo_revenue: DecimalRaw {
			value,
			precision: 38,
			scale: 12,
		},
		denominator_rows: 2,
		promo_rows: 1,
	}
}

#[test]
fn strict_q14_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q14(0.01))
		})
		.expect("spawn q14 correctness thread")
		.join()
		.expect("q14 correctness thread panicked");
	assert_eq!(attempt.query, "q14");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q14_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q14_oracle(0.01).expect("q14 oracle");
	assert!(
		oracle.denominator_rows >= 1,
		"denominator rows at sf=0.01: {}",
		oracle.denominator_rows
	);
	assert!(
		oracle.promo_rows >= 1,
		"PROMO rows at sf=0.01: {}",
		oracle.promo_rows
	);
	assert!(
		oracle.promo_revenue.value.is_some(),
		"a non-empty q14 oracle must produce a non-NULL promo_revenue"
	);
	assert_eq!(oracle.promo_revenue.precision, 38);
	assert_eq!(oracle.promo_revenue.scale, 12);
}

#[test]
fn strict_q14_output_schema_is_asserted() {
	assert!(assert_strict_q14_schema(&[q14_batch(&[Some(100)], 38, 12)]).is_ok());
	assert!(
		assert_strict_q14_schema(&[q14_batch(&[Some(100)], 38, 10)]).is_err(),
		"a non-measured scale must trip the schema gate"
	);
}

#[test]
fn q14_promo_branch_is_required() {
	let mut oracle = q14_oracle(Some(123));
	oracle.promo_rows = 0;
	assert!(
		compare_strict_q14(&[q14_batch(&[Some(123)], 38, 12)], &oracle).is_err(),
		"a denominator-only q14 oracle is a hollow CASE-true proof"
	);
}

#[test]
fn q14_ratio_uses_half_away_division() {
	assert_eq!(
		decimal_division_raw(1, 38, 8, 200_000_000, 38, 4)
			.expect("q14-shaped division")
			.value,
		Some(1),
		"the q14 ratio rounds 0.5 ulp away from zero, not toward zero"
	);
}

#[test]
fn q14_non_promo_rows_contribute_to_denominator() {
	let correct = decimal_division_raw(10 * 1_000_000, 38, 8, 30, 38, 4)
		.expect("ratio with promo + non-promo denominator")
		.value;
	let promo_only_denominator = decimal_division_raw(10 * 1_000_000, 38, 8, 10, 38, 4)
		.expect("ratio with wrong promo-only denominator")
		.value;
	assert_ne!(
		correct, promo_only_denominator,
		"excluding non-PROMO rows from the denominator changes q14's ratio"
	);
	let oracle = q14_oracle(correct);
	assert!(
		compare_strict_q14(&[q14_batch(&[promo_only_denominator], 38, 12)], &oracle).is_err(),
		"the comparator catches the classic CASE denominator bug"
	);
}

#[test]
fn q14_correctness_mismatch_is_fail_at_compare() {
	let oracle = q14_oracle(Some(123));
	assert!(
		compare_strict_q14(&[q14_batch(&[Some(123)], 38, 12)], &oracle).is_ok(),
		"exact raw match"
	);
	assert!(
		compare_strict_q14(&[q14_batch(&[Some(124)], 38, 12)], &oracle).is_err(),
		"an off-by-one promo_revenue is a mismatch"
	);
	let mut drifted_oracle = q14_oracle(Some(123));
	drifted_oracle.promo_revenue.scale = 10;
	assert!(
		compare_strict_q14(&[q14_batch(&[Some(123)], 38, 12)], &drifted_oracle).is_err(),
		"the oracle-side Decimal type is part of the exact comparison"
	);
}

#[test]
fn q14_null_or_empty_result_is_fail_at_compare() {
	assert!(
		compare_strict_q14(&[q14_batch(&[None], 38, 12)], &q14_oracle(Some(123))).is_err(),
		"a NULL output is fail@compare"
	);
	assert!(
		compare_strict_q14(&[q14_batch(&[], 38, 12)], &q14_oracle(Some(123))).is_err(),
		"zero rows is fail@compare"
	);
	assert!(
		compare_strict_q14(
			&[q14_batch(&[Some(123), Some(123)], 38, 12)],
			&q14_oracle(Some(123))
		)
		.is_err(),
		"q14 must emit exactly one row"
	);
	assert!(
		compare_strict_q14(&[q14_batch(&[Some(123)], 38, 12)], &q14_oracle(None)).is_err(),
		"a NULL oracle is rejected by the anti-empty guard"
	);
}

fn q17_batch(values: &[Option<i128>], precision: u8, scale: i8) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![Field::new(
		"avg_yearly",
		DataType::Decimal128(precision, scale),
		true,
	)]));
	RecordBatch::try_new(
		schema,
		vec![Arc::new(
			Decimal128Array::from(values.to_vec())
				.with_precision_and_scale(precision, scale)
				.expect("q17 avg_yearly decimal column"),
		)],
	)
	.expect("q17 batch")
}

fn q17_oracle(value: Option<i128>) -> Q17Oracle {
	Q17Oracle {
		avg_yearly: DecimalRaw {
			value,
			precision: 21,
			scale: 6,
		},
		eligible_parts: 1,
		contributing_rows: 1,
	}
}

#[test]
fn strict_q17_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q17())
		})
		.expect("spawn q17 correctness thread")
		.join()
		.expect("q17 correctness thread panicked");
	assert_eq!(attempt.query, "q17");
	assert_eq!(attempt.scale_factor, Q17_CORRECTNESS_SCALE);
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q17_oracle_is_non_vacuous_at_correctness_scale() {
	let oracle = strict_q17_oracle(Q17_CORRECTNESS_SCALE).expect("q17 oracle");
	assert!(
		oracle.eligible_parts >= 1,
		"eligible parts at sf={Q17_CORRECTNESS_SCALE}: {}",
		oracle.eligible_parts
	);
	assert!(
		oracle.contributing_rows >= 1,
		"contributing rows at sf={Q17_CORRECTNESS_SCALE}: {}",
		oracle.contributing_rows
	);
	assert!(
		oracle.avg_yearly.value.is_some(),
		"a non-empty q17 oracle must produce a non-NULL avg_yearly"
	);
	assert_eq!(oracle.avg_yearly.precision, 21);
	assert_eq!(oracle.avg_yearly.scale, 6);
}

#[test]
fn q17_decimal_division_rounds_half_away_from_zero() {
	assert_eq!(
		decimal_division_raw(1, 10, 0, 20_000, 10, 0)
			.expect("positive division")
			.value,
		Some(1),
		"0.00005 rounds to 0.0001, not truncation"
	);
	assert_eq!(
		decimal_division_raw(1, 10, 0, -20_000, 10, 0)
			.expect("negative division")
			.value,
		Some(-1),
		"a negative quotient rounds away from zero"
	);
	assert_eq!(
		decimal_division_raw(100, 10, 0, 0, 10, 0)
			.expect("zero divisor")
			.value,
		None,
		"the executor treats Decimal division by zero as NULL"
	);
}

#[test]
fn strict_q17_output_schema_is_asserted() {
	assert!(assert_strict_q17_schema(&[q17_batch(&[Some(100)], 21, 6)]).is_ok());
	assert!(
		assert_strict_q17_schema(&[q17_batch(&[Some(100)], 20, 6)]).is_err(),
		"a non-measured precision must trip the schema gate"
	);
}

#[test]
fn q17_correctness_mismatch_is_fail_at_compare() {
	let oracle = q17_oracle(Some(123));
	assert!(
		compare_strict_q17(&[q17_batch(&[Some(123)], 21, 6)], &oracle).is_ok(),
		"exact raw match"
	);
	assert!(
		compare_strict_q17(&[q17_batch(&[Some(124)], 21, 6)], &oracle).is_err(),
		"an off-by-one avg_yearly is a mismatch"
	);
	let mut drifted_oracle = q17_oracle(Some(123));
	drifted_oracle.avg_yearly.precision = 20;
	assert!(
		compare_strict_q17(&[q17_batch(&[Some(123)], 21, 6)], &drifted_oracle).is_err(),
		"the oracle-side Decimal type is part of the exact comparison"
	);
}

#[test]
fn q17_null_or_empty_result_is_fail_at_compare() {
	assert!(
		compare_strict_q17(&[q17_batch(&[None], 21, 6)], &q17_oracle(Some(123))).is_err(),
		"a NULL output is fail@compare at the non-empty correctness scale"
	);
	assert!(
		compare_strict_q17(&[q17_batch(&[], 21, 6)], &q17_oracle(Some(123))).is_err(),
		"zero rows is fail@compare"
	);
	assert!(
		compare_strict_q17(
			&[q17_batch(&[Some(123), Some(123)], 21, 6)],
			&q17_oracle(Some(123))
		)
		.is_err(),
		"q17 must emit exactly one row"
	);
	assert!(
		compare_strict_q17(&[q17_batch(&[Some(123)], 21, 6)], &q17_oracle(None)).is_err(),
		"a NULL oracle is rejected by the anti-empty guard"
	);
}

fn q20_row(name: &str, address: &str) -> Q20Row {
	Q20Row {
		s_name: name.to_string(),
		s_address: address.to_string(),
	}
}

fn q20_oracle(rows: &[Q20Row]) -> Q20Oracle {
	Q20Oracle {
		rows: rows.to_vec(),
		selected_partsupp_rows: rows.len().max(1),
		qualifying_suppliers: rows.len(),
		threshold_pass_rows: rows.len().max(1),
		threshold_fail_rows: 1,
		absent_group_rows: 1,
	}
}

fn q20_batch(rows: &[Q20Row]) -> RecordBatch {
	let nullable_rows: Vec<(Option<&str>, Option<&str>)> = rows
		.iter()
		.map(|row| (Some(row.s_name.as_str()), Some(row.s_address.as_str())))
		.collect();
	q20_nullable_batch(&nullable_rows)
}

fn q20_nullable_batch(rows: &[(Option<&str>, Option<&str>)]) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("s_name", DataType::Utf8, true),
		Field::new("s_address", DataType::Utf8, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.0).collect::<Vec<Option<&str>>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|row| row.1).collect::<Vec<Option<&str>>>(),
			)),
		],
	)
	.expect("q20 batch")
}

#[test]
fn strict_q20_matches_independent_oracle() {
	let attempt = std::thread::Builder::new()
		.stack_size(64 * 1024 * 1024)
		.spawn(|| {
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("runtime")
				.block_on(check_strict_q20(0.01))
		})
		.expect("spawn q20 correctness thread")
		.join()
		.expect("q20 correctness thread panicked");
	assert_eq!(attempt.query, "q20");
	assert_eq!(attempt.phase, Phase::Compare, "{}", attempt.human());
	assert_eq!(attempt.status, Status::Pass, "{}", attempt.human());
}

#[test]
fn strict_q20_oracle_is_non_vacuous_at_default_scale() {
	let oracle = strict_q20_oracle(0.01);
	assert!(
		!oracle.rows.is_empty(),
		"expected a non-empty q20 answer at sf=0.01"
	);
	assert!(
		oracle.selected_partsupp_rows > 0,
		"selected forest partsupp rows at sf=0.01: {}",
		oracle.selected_partsupp_rows
	);
	assert!(
		oracle.threshold_pass_rows > 0,
		"threshold pass rows at sf=0.01: {}",
		oracle.threshold_pass_rows
	);
	assert_eq!(
		oracle.threshold_pass_rows + oracle.threshold_fail_rows,
		oracle.selected_partsupp_rows,
		"every selected partsupp row must either pass or fail the threshold"
	);
	assert!(
		oracle.threshold_fail_rows > 0,
		"live q20 should exercise threshold failure rows at sf=0.01"
	);
	assert!(
		oracle.absent_group_rows > 0,
		"live q20 should exercise absent correlated groups at sf=0.01"
	);
	assert!(
		oracle.qualifying_suppliers >= oracle.rows.len(),
		"outer CANADA filter can only reduce the qualifying supplier set"
	);
}

#[test]
fn strict_q20_output_schema_is_asserted() {
	let row = q20_row("Supplier#000000001", "1 Main Street");
	assert!(assert_strict_q20_schema(&[q20_batch(&[row])]).is_ok());

	let drifted = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("s_name", DataType::Utf8, false),
			Field::new("s_address", DataType::Int64, false),
		])),
		vec![
			Arc::new(StringArray::from(vec!["Supplier#000000001"])) as ArrayRef,
			Arc::new(Int64Array::from(vec![1])) as ArrayRef,
		],
	)
	.expect("drifted q20 batch");
	assert!(
		assert_strict_q20_schema(&[drifted]).is_err(),
		"a non-measured address type must trip the schema gate"
	);
}

#[test]
fn q20_threshold_boundary_flip() {
	let sum_qty_raw = Some(200_i128);
	assert!(
		!q20_threshold_passes(1, sum_qty_raw),
		"strict > excludes equality: 1*10000 == 50*200"
	);
	assert!(
		q20_threshold_passes(2, sum_qty_raw),
		"a partsupp just above the 0.5*SUM threshold qualifies"
	);
}

#[test]
fn q20_absent_lineitem_group_follows_sql_null_rule() {
	assert!(
		!q20_threshold_passes(100, None),
		"SUM over an absent correlated group is NULL, so the comparison is unknown and fails"
	);
}

#[test]
fn q20_forest_is_prefix_match() {
	assert!(q20_part_name_is_forest_prefix("forest green"));
	assert!(
		!q20_part_name_is_forest_prefix("deep forest green"),
		"q20's LIKE 'forest%' is a prefix predicate, not contains"
	);
	assert!(
		!q20_part_name_is_forest_prefix("FOREST GREEN"),
		"the oracle follows the case-sensitive harness LIKE semantics"
	);
}

#[test]
fn q20_correctness_mismatch_is_fail_at_compare() {
	let row = q20_row("Supplier#000000001", "1 Main Street");
	let oracle = q20_oracle(std::slice::from_ref(&row));
	assert!(
		compare_strict_q20(&[q20_batch(std::slice::from_ref(&row))], &oracle).is_ok(),
		"exact supplier row match"
	);

	let wrong_address = q20_row("Supplier#000000001", "wrong address");
	assert!(
		compare_strict_q20(&[q20_batch(&[wrong_address])], &oracle).is_err(),
		"a wrong supplier address is a mismatch"
	);

	let missing = q20_row("Supplier#000000002", "2 Main Street");
	let two_row_oracle = q20_oracle(&[row.clone(), missing.clone()]);
	assert!(
		compare_strict_q20(&[q20_batch(std::slice::from_ref(&row))], &two_row_oracle).is_err(),
		"a dropped supplier is a mismatch"
	);

	let extra = q20_row("Supplier#000000003", "3 Main Street");
	assert!(
		compare_strict_q20(&[q20_batch(&[row.clone(), extra])], &oracle).is_err(),
		"an extra supplier is a mismatch"
	);

	assert!(
		compare_strict_q20(&[q20_batch(&[row.clone(), row])], &two_row_oracle).is_err(),
		"a duplicate emitted s_name is rejected"
	);
}

#[test]
fn q20_null_cell_is_fail_at_compare() {
	let oracle = q20_oracle(&[q20_row("Supplier#000000001", "1 Main Street")]);
	assert!(
		compare_strict_q20(
			&[q20_nullable_batch(&[(None, Some("1 Main Street"))])],
			&oracle
		)
		.is_err(),
		"a NULL s_name is fail@compare"
	);
	assert!(
		compare_strict_q20(
			&[q20_nullable_batch(&[(Some("Supplier#000000001"), None)])],
			&oracle
		)
		.is_err(),
		"a NULL s_address is fail@compare"
	);
}

#[test]
fn q20_order_violation_is_fail() {
	let row_a = q20_row("Supplier#000000001", "1 Main Street");
	let row_b = q20_row("Supplier#000000002", "2 Main Street");
	let oracle = q20_oracle(&[row_a.clone(), row_b.clone()]);
	assert!(
		compare_strict_q20(&[q20_batch(&[row_b, row_a])], &oracle).is_err(),
		"s_name descending violates ORDER BY s_name"
	);
}
