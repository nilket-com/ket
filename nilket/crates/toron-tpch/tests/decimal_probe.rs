//! ADR 262/263 verify-first probe (retained): the SQL-surface DDL gap (still rejected at plan), and
//! the load-bearing Decimal128 compute path — which ADR 263 moved from a bind wall to a clean execute.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Date32Array, Decimal128Array};
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
