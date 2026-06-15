//! ADR 241 — Window Functions Substrate architecture guards.
//!
//! Load-bearing proofs are the behavior gates (`window_test.rs`) and the distributed cardinal
//! (`distributed_process_test.rs`). These guards pin the structural facts: a new `Window` node (logical
//! + physical), a ranking-only `WindowFunction` enum, `OVER`-lowers-to-`Window`-over-`Exchange`,
//! row-preservation, the node-agnostic fragmentation reuse, the single-`OVER`-spec bound, the deferred
//! fences, and the absence of any optimizer/Wendelon surface.

use super::support::*;

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr241_adds_window_logical_and_physical_node() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	assert!(ir.contains("pub enum WindowFunction"));
	assert!(ir.contains("struct WindowFunctionCall"));
	let logical = source_block(&ir, "pub enum LogicalPlan");
	assert!(
		logical.contains("Window {"),
		"LogicalPlan must have a Window variant"
	);
	let physical = source_block(&execution, "pub enum ExecutionPlan");
	assert!(
		physical.contains("Window {"),
		"ExecutionPlan must have a Window variant"
	);
}

#[test]
pub(crate) fn adr241_window_function_enum_is_ranking_only() {
	let ir = source("src/ir.rs");
	let window_function = source_block(&ir, "pub enum WindowFunction");
	for variant in ["RowNumber", "Rank", "DenseRank"] {
		assert!(
			window_function.contains(variant),
			"WindowFunction must include {variant}"
		);
	}
	// Ranking-only: no aggregate-window variants in this cut.
	for deferred in ["Sum", "Avg", "Lag", "Lead", "Ntile", "FirstValue"] {
		assert!(
			!window_function.contains(deferred),
			"aggregate/offset window functions are deferred: {deferred}"
		);
	}
}

#[test]
pub(crate) fn adr241_over_spec_lowers_to_window_over_partition_exchange() {
	let planner = source("src/sql_planner.rs");
	let lower = source_block(&planner, "fn lower_select_window");
	assert!(lower.contains("LogicalPlan::Window"));
	assert!(
		lower.contains("LogicalPlan::Exchange") && lower.contains("key: partition_by"),
		"window lowers to a Window over an Exchange on the partition key"
	);
	// Empty PARTITION BY gathers to a single partition.
	assert!(
		lower.contains("partition_by.is_empty()")
			&& lower.contains("DEFAULT_AGGREGATE_PARTITION_COUNT")
	);
}

#[test]
pub(crate) fn adr241_window_is_row_preserving_not_an_aggregate() {
	// The binder builds the output schema as input fields + one rank column per function (N->N), not an
	// Aggregate (which reduces rows).
	let binder = binder_source();
	assert!(binder.contains("ExecutionPlan::Window"));
	assert!(
		binder.contains("Field::new(&call.output_name, DataType::Int64, false)"),
		"appends a non-null Int64 rank column per window function (row-preserving)"
	);
	assert!(
		binder.contains("PlanOptimizerFacts::composite_with_row"),
		"window facts are row-preserving (carry the input row bound)"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("window_preserves_input_row_count"));
}

#[test]
pub(crate) fn adr241_window_reuses_generic_exchange_boundary_fragmentation() {
	// Window must NOT add a strategy/arm to the join fragmenter — it rides the node-agnostic single
	// exchange boundary path. The fragmenter's strategy dispatch knows only Hash/Broadcast joins.
	let fragment = source("src/distributed/fragment.rs");
	let dispatch = source_block(&fragment, "fn fragment_join_plan");
	assert!(
		!dispatch.contains("Window"),
		"no Window arm in the join fragmenter dispatch"
	);
	assert!(
		!fragment.contains("fn fragment_window_plan"),
		"no dedicated window fragmenter"
	);
}

#[test]
pub(crate) fn adr241_single_over_spec_per_query_multiple_specs_deferred() {
	let planner = source("src/sql_planner.rs");
	let lower = source_block(&planner, "fn lower_select_window");
	assert!(
		lower.contains("window_specs_match") && lower.contains("multiple distinct OVER"),
		"window calls must share one OVER spec; multiple distinct specs are rejected"
	);
}

#[test]
pub(crate) fn adr241_frames_and_aggregate_window_stay_rejected() {
	let planner = source("src/sql_planner.rs");
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(extract.contains("explicit window frames are not supported"));
	assert!(extract.contains("WindowType::NamedWindow"));
	assert!(
		extract.contains("ROW_NUMBER") && extract.contains("DENSE_RANK"),
		"only ranking functions are accepted; others (aggregate-window) rejected"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("deferred_window_forms_stay_rejected"));
}

#[test]
pub(crate) fn adr241_distributed_window_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(distributed.contains("distributed_window_rank_is_value_discriminating"));
	let gate = source_block(
		&distributed,
		"async fn distributed_window_rank_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed window gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr241_window_node_is_frontend_agnostic_no_optimizer_or_wendelon_surface() {
	// The Window node introduces no optimizer-evidence or cross-ring surface: Wendelon and the Rune
	// engine carry no Window node or WindowFunction reference.
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("WindowFunction") || line.contains("ExecutionPlan::Window")
		})
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"window functions must not cross into Wendelon: {wendelon_offenders:?}"
	);
}
