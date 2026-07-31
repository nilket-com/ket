//! Invariant: strict q14 binding does not depend on optimizer facts — comma-join equality recognition keys
//! the join before strategy selection, so the same query binds with and without seeded facts.

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

#[tokio::test]
async fn strict_q14_binds_without_optimizer_facts() {
	let catalog = q14_catalog(false);
	let plan = SqlPlanner::new().plan(STRICT_Q14_SQL).expect("q14 plans");
	bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("strict q14 binds without optimizer facts");
}

#[tokio::test]
async fn strict_q14_binds_with_seeded_optimizer_facts() {
	let catalog = q14_catalog(true);
	let plan = SqlPlanner::new().plan(STRICT_Q14_SQL).expect("q14 plans");
	bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect("strict q14 binds with seeded optimizer facts");
}
