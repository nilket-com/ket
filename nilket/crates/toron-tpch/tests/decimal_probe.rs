//! ADR 262 verify-first probe (retained): where does strict Q6 (official physical types) block?
//! Two probes — the SQL-surface DDL gap (plan), and the load-bearing Decimal128 compute gap (bind).

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

/// The load-bearing compute gap: a `Decimal128(15,2)` fixture + strict Q6 binds past descriptor/plan
/// and blocks at BIND — decimal literals lower to `Float64`, so `l_discount BETWEEN 0.05 …` mismatches
/// the `Decimal128` column. This is the measured first red row.
#[tokio::test]
async fn decimal128_compute_blocks_at_bind() {
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

	// plan accepts the strict Q6 (the gap is not here either).
	let logical = SqlPlanner::new().plan(STRICT_Q6).expect("strict Q6 plans");

	// bind is the first wall: Decimal128 column vs Float64 literal.
	let error = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("strict Q6 should block at bind on the decimal/float type gap");
	let _ = Executor::new(storage); // executor never runs — bind blocks first
	assert!(
		format!("{error:?}").contains("Decimal128(15, 2)")
			&& format!("{error:?}").contains("Float64"),
		"the bind block is the Decimal column vs Float64 literal gap: {error:?}"
	);
}
