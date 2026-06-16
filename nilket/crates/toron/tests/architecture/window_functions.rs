//! ADR 241 — Window Functions Substrate architecture guards.
//!
//! Load-bearing proofs are the behavior gates (`window_test.rs`) and the distributed cardinal
//! (`distributed_process_test.rs`). These guards pin the structural facts: a `Window` node (logical +
//! physical), the `WindowFunction` enum (ranking + ADR 242 aggregate windows; offset/value deferred),
//! `OVER`-lowers-to-`Window`-over-`Exchange`, row-preservation, the node-agnostic fragmentation reuse,
//! the single-`OVER`-spec bound, the deferred fences (explicit frames → ADR 243), and the absence of any
//! optimizer/Wendelon surface.

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
pub(crate) fn adr241_window_function_enum_retains_ranking_variants() {
	// Reconciled for ADR 242: ranking variants stay, aggregate windows are now a variant, and the
	// offset/value window families remain deferred.
	let ir = source("src/ir.rs");
	let window_function = source_block(&ir, "pub enum WindowFunction");
	for variant in ["RowNumber", "Rank", "DenseRank"] {
		assert!(
			window_function.contains(variant),
			"WindowFunction must retain ranking variant {variant}"
		);
	}
	for deferred in ["Lag", "Lead", "Ntile", "FirstValue", "LastValue"] {
		assert!(
			!window_function.contains(deferred),
			"offset/value window functions are deferred: {deferred}"
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
pub(crate) fn adr241_named_windows_stay_rejected() {
	// Reconciled for ADR 243: explicit ROWS frames now plan (RANGE/GROUPS stay deferred — see
	// adr243_range_groups_and_ranking_frames_stay_rejected); named WINDOW references stay rejected.
	let planner = source("src/sql_planner.rs");
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(extract.contains("WindowType::NamedWindow"));
	assert!(extract.contains("named windows are not supported"));
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

// ---------------------------------------------------------------------------
// ADR 242 — aggregate window functions (default frame)
// ---------------------------------------------------------------------------

#[test]
pub(crate) fn adr242_window_call_carries_ranking_or_aggregate_function() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	let window_function = source_block(&ir, "pub enum WindowFunction");
	assert!(
		window_function.contains("Aggregate(AggregateFunction)"),
		"WindowFunction carries an aggregate kind"
	);
	let call = source_block(&ir, "pub struct WindowFunctionCall");
	assert!(
		call.contains("args: Vec<Expr>"),
		"window call carries arguments"
	);
	let bound = source_block(&execution, "pub struct BoundWindowCall");
	assert!(
		bound.contains("function: WindowFunction")
			&& bound.contains("arg: Option<BoundExprProgram>"),
		"bound window call carries the function and the optional bound aggregate argument"
	);
}

#[test]
pub(crate) fn adr242_aggregate_window_lowers_to_window_node_reusing_kernels() {
	let planner = source("src/sql_planner.rs");
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(
		extract.contains("lower_aggregate_function"),
		"aggregate windows reuse the ordinary aggregate lowering (kernels)"
	);
	assert!(
		extract.contains("WindowFunction::Aggregate"),
		"aggregate windows map to a Window-node aggregate call"
	);
	assert!(
		extract.contains("AggregateFunction::CountAll"),
		"COUNT(*) (arg-less) is a supported window aggregate"
	);
}

#[test]
pub(crate) fn adr242_default_frame_is_range_unbounded_preceding() {
	// The default-frame path finalizes the running accumulator per peer group (the RANGE default-frame
	// unit), so tied ORDER BY rows share the cumulative value. (Reconciled for ADR 243: the peer logic
	// now lives in `execute_window_aggregate_default`; `execute_window_aggregate` dispatches on frame.)
	let exec = source("src/exec.rs");
	let aggregate = source_block(&exec, "fn execute_window_aggregate_default");
	assert!(
		aggregate.contains("is_peer_group_end"),
		"the default window aggregate finalizes per peer group (RANGE), not row-by-row (ROWS)"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("running_window_range_ties_share_value"));
	assert!(tests.contains("default_frame_aggregate_window_unchanged"));
}

#[test]
pub(crate) fn adr242_window_aggregate_is_row_preserving() {
	let binder = binder_source();
	assert!(
		binder.contains("aggregate_field(&bound_agg"),
		"aggregate-window output column takes the aggregate result type/nullability"
	);
	assert!(
		binder.contains("PlanOptimizerFacts::composite_with_row"),
		"window facts stay row-preserving (carry the input row bound)"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("window_aggregate_preserves_row_count"));
}

#[test]
pub(crate) fn adr242_distinct_and_experimental_window_aggregates_stay_rejected() {
	// Reconciled for ADR 243: explicit ROWS frames now lower (the blanket frame reject is gone), but
	// DISTINCT and experimental window aggregates stay rejected.
	let planner = source("src/sql_planner.rs");
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(extract.contains("DISTINCT window aggregates are not supported"));
	assert!(
		extract.contains("only COUNT, SUM, MIN, MAX, and AVG"),
		"experimental window aggregates (LINEAR_REGRESS/APPROX_QUANTILE) stay rejected"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("deferred_window_forms_stay_rejected"));
}

#[test]
pub(crate) fn adr242_window_aggregate_reuses_partition_exchange_fragmentation() {
	// No dedicated window fragmenter and no Window arm in the join fragmenter — aggregate windows ride
	// the same node-agnostic exchange-boundary path ADR 241 introduced.
	let fragment = source("src/distributed/fragment.rs");
	assert!(
		!fragment.contains("fn fragment_window_plan"),
		"no dedicated window fragmenter"
	);
	let dispatch = source_block(&fragment, "fn fragment_join_plan");
	assert!(
		!dispatch.contains("Window"),
		"no Window arm in the join fragmenter dispatch"
	);
}

#[test]
pub(crate) fn adr242_distributed_window_aggregate_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(distributed.contains("distributed_window_aggregate_is_value_discriminating"));
	let gate = source_block(
		&distributed,
		"async fn distributed_window_aggregate_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed aggregate-window gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr242_no_new_physical_optimizer_durable_wendelon_or_rune_surface() {
	// Aggregate windows reuse the existing Window node + aggregate kernels: no new bound window surface
	// crosses into Wendelon.
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("BoundWindowCall") || line.contains("WindowFunction::Aggregate")
		})
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"aggregate windows must not cross into Wendelon: {wendelon_offenders:?}"
	);
}

// ---------------------------------------------------------------------------
// ADR 243 — rows window frames (explicit ROWS BETWEEN)
// ---------------------------------------------------------------------------

#[test]
pub(crate) fn adr243_window_call_carries_optional_rows_frame() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	assert!(ir.contains("pub enum WindowFrameBound"));
	assert!(ir.contains("pub struct WindowFrame"));
	let call = source_block(&ir, "pub struct WindowFunctionCall");
	assert!(
		call.contains("frame: Option<WindowFrame>"),
		"the logical window call carries an optional frame"
	);
	let bound = source_block(&execution, "pub struct BoundWindowCall");
	assert!(
		bound.contains("frame: Option<WindowFrame>"),
		"the bound window call carries the frame per-call (not on the Window node)"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("rows_frame_multiple_functions_can_differ_in_frame"));
}

#[test]
pub(crate) fn adr243_rows_frame_lowers_with_bounds_reusing_window_node() {
	// Reconciled for ADR 244: `lower_rows_window_frame` generalized to `lower_window_frame` (ROWS + RANGE).
	let planner = source("src/sql_planner.rs");
	assert!(planner.contains("fn lower_window_frame"));
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(
		extract.contains("self.lower_window_frame"),
		"an explicit frame lowers to the per-call frame payload"
	);
	assert!(
		planner.contains("WindowFrameBound::Preceding")
			&& planner.contains("WindowFrameBound::Following"),
		"the frame bounds map to the IR WindowFrameBound"
	);
}

#[test]
pub(crate) fn adr243_rows_frame_executor_is_sliding_not_peer_default() {
	let exec = source("src/exec.rs");
	assert!(exec.contains("fn execute_window_aggregate_rows"));
	let dispatch = source_block(&exec, "fn execute_window_aggregate(");
	assert!(
		dispatch.contains("execute_window_aggregate_rows")
			&& dispatch.contains("execute_window_aggregate_default"),
		"the window aggregate dispatches on the frame (default peer path vs. ROWS sliding path)"
	);
	let rows = source_block(&exec, "fn execute_window_aggregate_rows");
	assert!(
		rows.contains("window_frame_slice"),
		"the ROWS path re-aggregates a per-row physical slice, not the peer accumulator"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("rows_frame_running_does_not_share_ties"));
}

#[test]
pub(crate) fn adr243_rows_frame_window_is_row_preserving() {
	let binder = binder_source();
	assert!(
		binder.contains("frame: call.frame"),
		"the per-call frame threads into the bound window call"
	);
	assert!(binder.contains("PlanOptimizerFacts::composite_with_row"));
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("rows_frame_moving_window_edges_respect_offsets"));
}

#[test]
pub(crate) fn adr243_ranking_frames_stay_rejected() {
	// Reconciled for ADR 245: explicit RANGE (244) and GROUPS (245) now plan; frames on ranking
	// functions stay rejected (ranking is frame-independent).
	let planner = source("src/sql_planner.rs");
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(
		extract.contains("window frames are not supported on ranking functions"),
		"frames on ranking functions stay rejected"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("deferred_window_forms_stay_rejected"));
}

#[test]
pub(crate) fn adr243_rows_frame_reuses_partition_exchange_fragmentation() {
	let fragment = source("src/distributed/fragment.rs");
	assert!(
		!fragment.contains("fn fragment_window_plan"),
		"no dedicated window fragmenter"
	);
	let dispatch = source_block(&fragment, "fn fragment_join_plan");
	assert!(
		!dispatch.contains("Window"),
		"no Window arm in the join fragmenter dispatch"
	);
}

#[test]
pub(crate) fn adr243_rows_frame_distributed_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(distributed.contains("rows_frame_distributed_gate_is_value_discriminating"));
	let gate = source_block(
		&distributed,
		"async fn rows_frame_distributed_gate_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed ROWS-frame gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr243_no_new_physical_optimizer_durable_wendelon_or_rune_surface() {
	// ROWS frames reuse the existing Window node + aggregate kernels: the frame payload does not cross
	// into Wendelon.
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("WindowFrame") || line.contains("WindowFrameBound"))
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"window frames must not cross into Wendelon: {wendelon_offenders:?}"
	);
}

// ---------------------------------------------------------------------------
// ADR 244 — range window frames (explicit RANGE BETWEEN)
// ---------------------------------------------------------------------------

#[test]
pub(crate) fn adr244_frame_unit_is_first_class_with_typed_value_delta() {
	let ir = source("src/ir.rs");
	assert!(ir.contains("pub enum WindowFrameUnit"));
	let delta = source_block(&ir, "pub enum WindowFrameDelta");
	assert!(
		delta.contains("Rows(u64)") && delta.contains("Range(ScalarValue)"),
		"a bound delta is a ROWS row count or a typed RANGE value"
	);
	let frame = source_block(&ir, "pub struct WindowFrame");
	assert!(
		frame.contains("unit: WindowFrameUnit"),
		"the frame carries a first-class unit"
	);
}

#[test]
pub(crate) fn adr244_range_frame_lowers_with_typed_offsets_matched_to_key() {
	let planner = source("src/sql_planner.rs");
	let delta = source_block(&planner, "fn lower_frame_delta");
	assert!(
		delta.contains("WindowFrameUnit::Range") && delta.contains("self.lower_expr"),
		"a RANGE offset lowers a typed value literal (number or INTERVAL)"
	);
	let binder = binder_source();
	assert!(
		binder.contains("fn range_offset_compatible"),
		"the binder checks the RANGE offset type against the ORDER BY key"
	);
}

#[test]
pub(crate) fn adr244_range_frame_bound_reuses_existing_typed_arithmetic() {
	let exec = source("src/exec.rs");
	let bound = source_block(&exec, "fn range_bound_value_rows");
	assert!(
		bound.contains("evaluate_unbound_arithmetic"),
		"the RANGE bound `key ± offset` reuses the existing typed arithmetic (numeric + temporal)"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("temporal_range_interval_offset_windows_by_value"));
}

#[test]
pub(crate) fn adr244_range_frame_window_is_row_preserving() {
	let exec = source("src/exec.rs");
	assert!(exec.contains("fn execute_window_aggregate_range"));
	let binder = binder_source();
	assert!(binder.contains("PlanOptimizerFacts::composite_with_row"));
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("range_offset_extends_across_peer_groups"));
}

#[test]
pub(crate) fn adr244_non_arithmetic_value_offset_keys_and_multikey_range_stay_rejected() {
	// Reconciled for ADR 245: GROUPS now plans; multi-key RANGE value offsets and non-arithmetic-key
	// RANGE value offsets stay rejected.
	let planner = source("src/sql_planner.rs");
	assert!(planner.contains("RANGE value offsets require exactly one ORDER BY column"));
	let binder = binder_source();
	assert!(
		binder.contains("is not compatible with ORDER BY key"),
		"a RANGE value offset on a non-arithmetic key is rejected at bind"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("range_value_offset_on_non_arithmetic_key_rejected"));
}

#[test]
pub(crate) fn adr244_range_frame_reuses_partition_exchange_fragmentation() {
	let fragment = source("src/distributed/fragment.rs");
	assert!(
		!fragment.contains("fn fragment_window_plan"),
		"no dedicated window fragmenter"
	);
	let dispatch = source_block(&fragment, "fn fragment_join_plan");
	assert!(
		!dispatch.contains("Window"),
		"no Window arm in the join fragmenter dispatch"
	);
}

#[test]
pub(crate) fn adr244_range_frame_distributed_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(distributed.contains("range_frame_distributed_gate_is_value_discriminating"));
	let gate = source_block(
		&distributed,
		"async fn range_frame_distributed_gate_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed RANGE gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr244_no_new_physical_optimizer_durable_wendelon_or_rune_surface() {
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("WindowFrameUnit") || line.contains("WindowFrameDelta")
		})
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"range frames must not cross into Wendelon: {wendelon_offenders:?}"
	);
}

// ---------------------------------------------------------------------------
// ADR 245 — groups window frames (explicit GROUPS BETWEEN)
// ---------------------------------------------------------------------------

#[test]
pub(crate) fn adr245_frame_unit_enum_completes_with_groups() {
	let ir = source("src/ir.rs");
	let unit = source_block(&ir, "pub enum WindowFrameUnit");
	assert!(
		unit.contains("Rows") && unit.contains("Range") && unit.contains("Groups"),
		"the explicit frame-unit family is complete (Rows/Range/Groups)"
	);
	let delta = source_block(&ir, "pub enum WindowFrameDelta");
	assert!(
		delta.contains("Groups(u64)"),
		"GROUPS carries a peer-group count delta"
	);
}

#[test]
pub(crate) fn adr245_groups_frame_lowers_with_peer_group_count_offset() {
	let planner = source("src/sql_planner.rs");
	let delta = source_block(&planner, "fn lower_frame_delta");
	assert!(
		delta.contains("WindowFrameUnit::Groups")
			&& delta.contains("WindowFrameDelta::Groups(window_frame_offset"),
		"a GROUPS offset lowers to a non-negative integer peer-group count"
	);
}

#[test]
pub(crate) fn adr245_groups_frame_executor_is_peer_group_count_not_row_or_value() {
	let exec = source("src/exec.rs");
	assert!(exec.contains("fn execute_window_aggregate_groups"));
	let groups = source_block(&exec, "fn execute_window_aggregate_groups");
	assert!(
		groups.contains("group_id") && groups.contains("group_start_row"),
		"GROUPS slices by peer-group ordinal, not physical row or value distance"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("groups_offset_differs_from_rows_and_range"));
}

#[test]
pub(crate) fn adr245_groups_frame_window_is_row_preserving() {
	let exec = source("src/exec.rs");
	assert!(exec.contains("fn execute_window_aggregate_groups"));
	let binder = binder_source();
	assert!(binder.contains("PlanOptimizerFacts::composite_with_row"));
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("groups_frame_includes_whole_peer_group_and_tied_rows_share"));
}

#[test]
pub(crate) fn adr245_exclude_no_order_by_and_ranking_frames_stay_rejected() {
	let planner = source("src/sql_planner.rs");
	assert!(planner.contains("GROUPS count offsets require an ORDER BY"));
	let extract = source_block(&planner, "fn extract_window_call");
	assert!(
		extract.contains("window frames are not supported on ranking functions"),
		"frames on ranking functions stay rejected"
	);
	let tests = source("tests/window_test.rs");
	assert!(tests.contains("deferred_window_forms_stay_rejected"));
}

#[test]
pub(crate) fn adr245_groups_frame_reuses_partition_exchange_fragmentation() {
	let fragment = source("src/distributed/fragment.rs");
	assert!(
		!fragment.contains("fn fragment_window_plan"),
		"no dedicated window fragmenter"
	);
	let dispatch = source_block(&fragment, "fn fragment_join_plan");
	assert!(
		!dispatch.contains("Window"),
		"no Window arm in the join fragmenter dispatch"
	);
}

#[test]
pub(crate) fn adr245_groups_frame_distributed_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(distributed.contains("groups_frame_distributed_gate_is_value_discriminating"));
	let gate = source_block(
		&distributed,
		"async fn groups_frame_distributed_gate_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed GROUPS gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr245_no_new_physical_optimizer_durable_wendelon_or_rune_surface() {
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("WindowFrameUnit::Groups") || line.contains("WindowFrameDelta::Groups")
		})
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"group frames must not cross into Wendelon: {wendelon_offenders:?}"
	);
}
