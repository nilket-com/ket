//! ADR 262/263 verify-first probe (retained): the SQL-surface DDL gap (still rejected at plan), and
//! the load-bearing Decimal128 compute path — which ADR 263 moved from a bind wall to a clean execute.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Date32Array, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::exec::Executor;
use toron::ir::TableId;
use toron::sql_planner::SqlPlanner;
use toron_tpch::HarnessScan;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

const STRICT_Q6: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
	FROM lineitem \
	WHERE l_shipdate >= DATE '1994-01-01' \
	  AND l_shipdate <  DATE '1995-01-01' \
	  AND l_discount BETWEEN 0.05 AND 0.07 \
	  AND l_quantity < 24";

/// The SQL-surface Decimal gap: declaring official `DECIMAL` columns rejects at plan.
#[test]
fn decimal_ddl_blocks_at_plan() {
	let error = SqlPlanner::new()
		.plan("CREATE TABLE lineitem (l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_shipdate DATE)")
		.expect_err("DECIMAL DDL should reject at plan");
	assert!(
		error
			.to_string()
			.contains("unsupported CREATE TABLE column type: DECIMAL"),
		"{error}"
	);
}

/// The load-bearing compute path: a `Decimal128(15,2)` fixture + strict Q6 binds past descriptor/plan
/// and now EXECUTES (ADR 263). Decimal comparison literals coerce to `Decimal128` at bind, `*` widens
/// to `Decimal128(31,4)`, and `SUM` carries that type. Only row 0 survives `l_quantity < 24`, so
/// `revenue = 100.00 * 0.06 = 6.0000` → `Decimal128(31,4)` raw `60000`. The wall ADR 262 measured at
/// bind is gone; this probe is now the regression test that the decimal Q6 slice computes.
#[tokio::test]
async fn decimal128_compute_binds_and_executes() {
	let (catalog, storage) = lineitem_fixture();

	// plan accepts the strict Q6.
	let logical = SqlPlanner::new().plan(STRICT_Q6).expect("strict Q6 plans");

	// bind now succeeds: decimal comparison literals coerce to Decimal128(15,2).
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 263: strict Q6 binds on the Decimal128 fixture");

	// execute computes the revenue over the surviving row(s).
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 263: strict Q6 executes on the Decimal128 fixture");

	let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
	assert_eq!(rows, 1, "the global SUM produces a single revenue row");
	let revenue = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("a non-empty result batch")
		.column(0);
	assert_eq!(
		revenue.data_type(),
		&DataType::Decimal128(31, 4),
		"`*` widens to Decimal128(31,4) and SUM carries it"
	);
	let revenue = revenue
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("revenue is a Decimal128 column");
	assert_eq!(
		revenue.value(0),
		60_000,
		"only row 0 survives l_quantity < 24: 100.00 * 0.06 = 6.0000 (raw 60000 at scale 4)"
	);
}

/// ADR 263 review (finding 1): grouped SUM(Decimal128) must EXECUTE, not bind-then-fail. GROUP BY
/// l_shipdate over the fixture yields two single-row groups → revenue {100.00, 200.00}. This would fail
/// under the pre-fix grouped finalize (which rejected Decimal as an unsupported SUM output type).
#[tokio::test]
async fn grouped_decimal_sum_executes() {
	let (catalog, storage) = lineitem_fixture();
	let logical = SqlPlanner::new()
		.plan(
			"SELECT l_shipdate, SUM(l_extendedprice) AS revenue FROM lineitem GROUP BY l_shipdate",
		)
		.expect("grouped decimal SUM plans");
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("grouped decimal SUM binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 263: grouped decimal SUM executes (not bind-then-fail)");

	let mut revenue = Vec::new();
	for batch in &batches {
		let column = batch.column_by_name("revenue").expect("revenue column");
		assert_eq!(
			column.data_type(),
			&DataType::Decimal128(15, 2),
			"grouped SUM(Decimal128(15,2)) carries the input (p,s)"
		);
		let column = column
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("revenue is Decimal128");
		revenue.extend((0..column.len()).map(|i| column.value(i)));
	}
	revenue.sort_unstable();
	assert_eq!(
		revenue,
		vec![10_000, 20_000],
		"two groups: 100.00 and 200.00 (raw at scale 2)"
	);
}

/// ADR 263 review (finding 2): a literal that does not fit the column's Decimal128(15,2) precision is a
/// clean bind error — not a wrong value or a panic at execute. Would panic/overflow under the pre-fix
/// unchecked literal helpers.
#[tokio::test]
async fn oversized_decimal_literal_rejects_at_bind() {
	let (catalog, _storage) = lineitem_fixture();
	let logical = SqlPlanner::new()
		.plan("SELECT l_extendedprice FROM lineitem WHERE l_extendedprice < 999999999999999999")
		.expect("plan accepts the comparison shape");
	let error = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("an oversized decimal literal rejects at bind");
	assert!(
		format!("{error:?}").contains("does not fit Decimal128(15, 2)"),
		"clean precision-fit bind error: {error:?}"
	);
}

/// ADR 266: decimal `+`/`-` with literal coercion binds + executes. `1 - l_discount` coerces the integer
/// literal to `Decimal128(15,2)` and subtracts to `Decimal128(16,2)`; the q01 `disc_price` chain
/// (`SUM(l_extendedprice * (1 - l_discount))`) runs coercion → `-` → `*` → `SUM` end-to-end. (q01
/// *nominates* this op but blocks earlier on multiple-aggregate projection — ADR 267.)
#[tokio::test]
async fn decimal_add_sub_binds_and_executes() {
	let (catalog, storage) = lineitem_fixture();

	// `1 - l_discount` -> Decimal128(16,2): 1.00-0.06=0.94, 1.00-0.05=0.95 (raw 94, 95 at scale 2).
	let plan = SqlPlanner::new()
		.plan("SELECT (1 - l_discount) AS d FROM lineitem")
		.expect("plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 266: decimal sub binds (literal coerces to Decimal)");
	let batches = Executor::new(storage.clone())
		.execute_bound(&bound, None)
		.await
		.expect("ADR 266: decimal sub executes");
	let column = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("rows")
		.column(0);
	assert_eq!(
		column.data_type(),
		&DataType::Decimal128(16, 2),
		"`1 - decimal` carries the scale and adds one precision digit"
	);
	let column = column
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("Decimal128");
	let mut values: Vec<i128> = (0..column.len()).map(|i| column.value(i)).collect();
	values.sort_unstable();
	assert_eq!(values, vec![94, 95], "1.00-0.06=0.94, 1.00-0.05=0.95");

	// q01's disc_price chain: coercion -> `-` -> `*` -> SUM, end-to-end.
	let plan = SqlPlanner::new()
		.plan("SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem")
		.expect("plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 266: coercion -> sub -> mul -> SUM binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 266: disc_price chain executes");
	let revenue = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("a revenue row")
		.column(0);
	let revenue = revenue
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("revenue is Decimal128");
	// 100.00*0.94 + 200.00*0.95 = 94.0000 + 190.0000 = 284.0000 at scale 4 (Decimal128(32,4)).
	assert_eq!(
		revenue.value(0),
		2_840_000,
		"SUM(price*(1-discount)) = 284.0000"
	);
}

/// ADR 302: Decimal +/- now follows Arrow's mixed-(p,s) add/sub typing. q09's profit expression is exactly
/// this shape: a `Decimal128(32,4)` revenue term minus a `Decimal128(31,4)` cost term.
#[tokio::test]
async fn decimal_add_sub_aligns_mixed_precision_and_scale() {
	let (catalog, storage) = lineitem_fixture();

	let plan = SqlPlanner::new()
		.plan(
			"SELECT l_extendedprice * (1 - l_discount) - l_extendedprice * l_discount AS amount \
			 FROM lineitem",
		)
		.expect("plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("mixed-precision Decimal sub binds");
	let batches = Executor::new(storage.clone())
		.execute_bound(&bound, None)
		.await
		.expect("mixed-precision Decimal sub executes");
	let column = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("rows")
		.column(0);
	assert_eq!(
		column.data_type(),
		&DataType::Decimal128(33, 4),
		"Decimal128(32,4)-Decimal128(31,4) -> Decimal128(33,4)"
	);
	let column = column
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("Decimal128");
	let mut values: Vec<i128> = (0..column.len()).map(|i| column.value(i)).collect();
	values.sort_unstable();
	assert_eq!(values, vec![880_000, 1_800_000], "88.0000 and 180.0000");

	let plan = SqlPlanner::new()
		.plan("SELECT l_extendedprice + l_discount * l_discount AS amount FROM lineitem")
		.expect("plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("mixed-scale Decimal add binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("mixed-scale Decimal add executes");
	let column = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("rows")
		.column(0);
	assert_eq!(
		column.data_type(),
		&DataType::Decimal128(32, 4),
		"Decimal128(15,2)+Decimal128(31,4) -> Decimal128(32,4)"
	);
}

/// Non-literal Decimal comparison operands with different `(p,s)` reconcile through a binder-inserted,
/// value-preserving Decimal cast.
#[tokio::test]
async fn decimal_comparison_rescales_non_literal_operands() {
	let (catalog, storage) = lineitem_fixture();
	let plan = SqlPlanner::new()
		.plan("SELECT l_quantity FROM lineitem WHERE l_quantity < (l_extendedprice * 0.2)")
		.expect("decimal comparison plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 283: mismatched Decimal comparison operands bind");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 283: mismatched Decimal comparison operands execute");
	let mut values = Vec::new();
	for batch in &batches {
		let column = batch.column(0);
		assert_eq!(column.data_type(), &DataType::Decimal128(15, 2));
		let column = column
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("quantity Decimal128");
		values.extend((0..column.len()).map(|i| column.value(i)));
	}
	values.sort_unstable();
	assert_eq!(
		values,
		vec![1_000, 3_000],
		"10.00 < 20.0000 and 30.00 < 40.0000"
	);
}

/// A Decimal comparison whose value-preserving common type would exceed Decimal128's 38 digit precision
/// rejects cleanly at bind: no silent cap, no execute-time cast surprise.
#[tokio::test]
async fn decimal_comparison_precision_overflow_rejects_at_bind() {
	let (catalog, _storage) = wide_decimal_fixture();
	let plan = SqlPlanner::new()
		.plan("SELECT a FROM wide_decimal WHERE a < b")
		.expect("wide decimal comparison plans");
	let error = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("common Decimal comparison precision >38 rejects");
	assert!(
		format!("{error:?}").contains("Decimal comparison operands exceed precision 38"),
		"clean precision-overflow bind error: {error:?}"
	);
}

/// Decimal `/` gets the same literal coercion as `+`, `-`, and `*`, so `SUM(decimal) / 7.0` binds to
/// Decimal/Decimal and executes through the checked Decimal division kernel.
#[tokio::test]
async fn decimal_division_coerces_float_literal() {
	let (catalog, storage) = lineitem_fixture();
	let plan = SqlPlanner::new()
		.plan("SELECT SUM(l_extendedprice) / 7.0 AS yearly FROM lineitem")
		.expect("decimal division plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 283: Decimal / Float literal binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 283: Decimal / Float literal executes");
	let yearly = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("a result row")
		.column(0);
	assert_eq!(
		yearly.data_type(),
		&DataType::Decimal128(21, 6),
		"Decimal128(15,2) / Decimal128(15,2) -> Decimal128(21,6)"
	);
	let yearly = yearly
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("yearly Decimal128");
	assert!(!yearly.is_null(0), "non-zero divisor produces a value");
	assert_eq!(
		yearly.value(0),
		42_857_143,
		"(100.00 + 200.00) / 7.00 = 42.857143"
	);
}

/// ADR 268: AVG(Decimal) binds and executes, finalizing to a widened `Decimal128(p+4, s+4)`. Over the
/// fixture: AVG(l_extendedprice) = (100.00 + 200.00)/2 = 150.000000; AVG(l_discount) = (0.06 + 0.05)/2 =
/// 0.055000. The `Decimal128(19,6)` output type discriminates against a Float64 collapse, and the
/// 0.055000 value against a same-scale (scale-2) average — which would round 0.055 to 0.06. Two same-
/// function AVGs in one projection also exercise ADR 267's aliased disambiguation.
#[tokio::test]
async fn decimal_avg_binds_and_executes() {
	let (catalog, storage) = lineitem_fixture();
	let plan = SqlPlanner::new()
		.plan("SELECT AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc FROM lineitem")
		.expect("decimal AVG plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("ADR 268: decimal AVG binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 268: decimal AVG executes");
	let batch = batches
		.iter()
		.find(|batch| batch.num_rows() > 0)
		.expect("a result row");

	let avg_price = batch.column_by_name("avg_price").expect("avg_price column");
	assert_eq!(
		avg_price.data_type(),
		&DataType::Decimal128(19, 6),
		"AVG(Decimal128(15,2)) widens to Decimal128(19,6)"
	);
	let avg_price = avg_price
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("avg_price is Decimal128");
	assert_eq!(avg_price.value(0), 150_000_000, "150.000000 at scale 6");

	let avg_disc = batch.column_by_name("avg_disc").expect("avg_disc column");
	let avg_disc = avg_disc
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("avg_disc is Decimal128");
	assert_eq!(
		avg_disc.value(0),
		55_000,
		"0.055000 — not a scale-2 average rounded to 0.06"
	);
}

/// ADR 268: grouped AVG(Decimal) executes per group, in lockstep with grouped SUM. GROUP BY l_shipdate
/// gives two single-row groups, so each group's average is that row: {100.000000, 200.000000}.
#[tokio::test]
async fn grouped_decimal_avg_executes() {
	let (catalog, storage) = lineitem_fixture();
	let plan = SqlPlanner::new()
		.plan(
			"SELECT l_shipdate, AVG(l_extendedprice) AS avg_price FROM lineitem GROUP BY l_shipdate",
		)
		.expect("grouped decimal AVG plans");
	let bound = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("grouped decimal AVG binds");
	let batches = Executor::new(storage)
		.execute_bound(&bound, None)
		.await
		.expect("ADR 268: grouped decimal AVG executes");
	let mut avgs = Vec::new();
	for batch in &batches {
		let column = batch.column_by_name("avg_price").expect("avg_price column");
		assert_eq!(
			column.data_type(),
			&DataType::Decimal128(19, 6),
			"grouped AVG(Decimal128(15,2)) widens to Decimal128(19,6)"
		);
		let column = column
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("avg_price is Decimal128");
		avgs.extend((0..column.len()).map(|i| column.value(i)));
	}
	avgs.sort_unstable();
	assert_eq!(
		avgs,
		vec![100_000_000, 200_000_000],
		"single-row groups: 100.000000 and 200.000000 at scale 6"
	);
}

/// The Decimal128(15,2) `lineitem` fixture shared by the decimal probes: two rows —
/// (q=10.00, price=100.00, disc=0.06, ship=8900) and (q=30.00, price=200.00, disc=0.05, ship=8950).
fn lineitem_fixture() -> (MockCatalog, Arc<HarnessScan>) {
	let decimal = |name: &str| Field::new(name, DataType::Decimal128(15, 2), false);
	let schema = Arc::new(Schema::new(vec![
		decimal("l_quantity"),
		decimal("l_extendedprice"),
		decimal("l_discount"),
		Field::new("l_shipdate", DataType::Date32, false),
	]));
	let dec = |values: Vec<i128>| {
		Arc::new(
			Decimal128Array::from(values)
				.with_precision_and_scale(15, 2)
				.expect("decimal column"),
		) as arrow::array::ArrayRef
	};
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![
			dec(vec![1000, 3000]),
			dec(vec![10000, 20000]),
			dec(vec![6, 5]),
			Arc::new(Date32Array::from(vec![8900, 8950])),
		],
	)
	.expect("decimal128 batch");

	// descriptor + registration accept Decimal128 (the gap is not here).
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([2; 16]),
		1,
		"default",
		"lineitem",
		schema.as_ref(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("Decimal128 descriptor is accepted");
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(2), descriptor)
		.expect("Decimal128 registration is accepted");
	let storage = Arc::new(HarnessScan {
		batches: HashMap::from([(TableId(2), batch)]),
	});
	(catalog, storage)
}

fn wide_decimal_fixture() -> (MockCatalog, Arc<HarnessScan>) {
	let schema = Arc::new(Schema::new(vec![
		Field::new("a", DataType::Decimal128(38, 0), false),
		Field::new("b", DataType::Decimal128(38, 36), false),
	]));
	let dec = |values: Vec<i128>, precision: u8, scale: i8| {
		Arc::new(
			Decimal128Array::from(values)
				.with_precision_and_scale(precision, scale)
				.expect("wide decimal column"),
		) as arrow::array::ArrayRef
	};
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![dec(vec![1], 38, 0), dec(vec![1], 38, 36)],
	)
	.expect("wide decimal batch");
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([3; 16]),
		1,
		"default",
		"wide_decimal",
		schema.as_ref(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("wide Decimal128 descriptor is accepted");
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(3), descriptor)
		.expect("wide Decimal128 registration is accepted");
	let storage = Arc::new(HarnessScan {
		batches: HashMap::from([(TableId(3), batch)]),
	});
	(catalog, storage)
}
