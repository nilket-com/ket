//! ADR 269/270 grounding probe. ADR 269 found q14's comma join lowered to a keyless CROSS JOIN whose
//! admission depended on optimizer facts (a tiny broadcast-eligible `part`). ADR 270's binder-side
//! recognition lifts the `l_partkey = p_partkey` equality into a keyed inner join **before** strategy
//! selection, so the join is now keyed regardless of facts — and q14's real wall is the CASE
//! Decimal-vs-integer gap in both cases. These two tests pin that fact-independence.

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

/// ADR 270: even without optimizer facts, the comma-join equality is lifted into a keyed inner join, so
/// q14 no longer walls at the keyless CROSS JOIN — it reaches the CASE Decimal-vs-integer gap, exactly like
/// the seeded-facts case. (Pre-270 this walled at `CROSS JOIN`; recognition made the join fact-independent.)
#[tokio::test]
async fn q14_recognized_keyed_inner_without_optimizer_facts() {
	let catalog = q14_catalog(false);
	let plan = SqlPlanner::new().plan(STRICT_Q14_SQL).expect("q14 plans");
	let error = bind(&plan, &catalog, &PlannerConfig::default())
		.await
		.expect_err("no-facts q14 walls at the CASE decimal gap, not the cross-join");
	let message = format!("{error:?}");
	assert!(
		!message.contains("CROSS JOIN"),
		"recognition lifts the equi into keys, so no keyless-cross wall even without facts: {message}"
	);
	assert!(
		message.contains("CASE result branches cannot be unified"),
		"the real wall is CASE Decimal unification, independent of optimizer facts: {message}"
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
