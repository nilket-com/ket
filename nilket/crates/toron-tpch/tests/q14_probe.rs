//! ADR 269 verify-first probe (kept): grounds why honest optimizer facts matter for q14's measured wall.
//! Without facts the keyless comma-join is rejected as a CROSS JOIN outright; with honest facts the tiny
//! `part` is broadcast-eligible, the join is admitted, and q14's real wall is the CASE Decimal-vs-integer
//! gap — the row the harness records.

use arrow::record_batch::RecordBatch;

use toron::binder::{PlannerConfig, bind};
use toron::catalog::{MockCatalog, TableOptimizerFacts, TableRef};
use toron::distributed::budget::ByteCount;
use toron::distributed::task::RowCount;
use toron::ir::TableId;
use toron::sql_planner::SqlPlanner;
use toron_tpch::{STRICT_Q14_SQL, generate_part, generate_strict_lineitem};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
};

fn descriptor(id: u8, name: &str, batch: &RecordBatch) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id; 16]),
		1,
		"default",
		name,
		batch.schema().as_ref(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

fn q14_catalog(seed_facts: bool) -> MockCatalog {
	let lineitem = generate_strict_lineitem(0.0001);
	let part = generate_part(0.0001);
	let mut catalog = MockCatalog::new();
	for (id, name, batch) in [(1u64, "lineitem", &lineitem), (2u64, "part", &part)] {
		catalog
			.register_descriptor(TableId(id), descriptor(id as u8, name, batch))
			.expect("register descriptor");
		if seed_facts {
			catalog
				.seed_optimizer_facts(
					&TableRef::new(name),
					TableOptimizerFacts {
						row_upper_bound: Some(RowCount::new(batch.num_rows() as u64)),
						byte_upper_bound: Some(
							ByteCount::new(batch.get_array_memory_size() as u64),
						),
						..Default::default()
					},
				)
				.expect("seed optimizer facts");
		}
	}
	catalog
}

/// Without optimizer facts, the comma-join's equi-predicate is never lifted into a join key and the
/// keyless CROSS JOIN is rejected — a fixture artifact, not q14's real wall.
#[tokio::test]
async fn q14_without_optimizer_facts_walls_at_cross_join() {
	let catalog = q14_catalog(false);
	let plan = SqlPlanner::new().plan(STRICT_Q14_SQL).expect("q14 plans");
	let error = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("no-facts q14 walls at the keyless cross-join");
	let message = format!("{error:?}");
	assert!(
		message.contains("CROSS JOIN"),
		"without optimizer facts the keyless cross-join is rejected: {message}"
	);
}

/// With honest facts the tiny `part` is broadcast-eligible, the join is admitted, and strict q14's real
/// first wall is the CASE Decimal-vs-integer (`ELSE 0`) result unification — the row the harness records.
#[tokio::test]
async fn q14_with_seeded_facts_walls_at_decimal_case() {
	let catalog = q14_catalog(true);
	let plan = SqlPlanner::new().plan(STRICT_Q14_SQL).expect("q14 plans");
	let error = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("seeded-facts strict q14 walls at the CASE decimal gap");
	let message = format!("{error:?}");
	assert!(
		message.contains("CASE result branches cannot be unified"),
		"seeded facts admit the join; the real wall is CASE Decimal unification: {message}"
	);
}
