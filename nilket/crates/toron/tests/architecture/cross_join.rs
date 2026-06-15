//! ADR 239 — CROSS JOIN substrate architecture guards.
//!
//! The load-bearing proofs are the behavior tests (`cross_join_test.rs`, the executor cartesian in
//! `hash_join_kinds_test.rs`, and the distributed cardinal in `distributed_process_test.rs`). These
//! guards pin the structural facts those tests rely on: a new join *kind* (not a new operator),
//! keyless lowering, broadcast-only strategy with fail-loud, an explicit match-all executor path,
//! the swap+restore column-order path, and the absence of any optimizer/durable/Wendelon/Rune surface.

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
pub(crate) fn adr239_adds_join_kind_cross_without_new_physical_operator() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");

	// D1: Cross is a new variant of the existing JoinKind enum.
	let join_kind = source_block(&ir, "pub enum JoinKind");
	assert!(
		join_kind.contains("Cross"),
		"JoinKind enum must include the Cross variant"
	);
	// Cross rides ExecutionPlan::Join — it must NOT add a new physical operator variant.
	assert!(
		!execution.contains("Cross"),
		"CROSS must not introduce a new ExecutionPlan operator; it rides ExecutionPlan::Join"
	);
}

#[test]
pub(crate) fn adr239_cross_join_and_comma_lower_to_keyless_cross() {
	let planner = source("src/sql_planner.rs");
	let lower_join_operator = source_block(&planner, "fn lower_join_operator");

	// D2: bare CROSS JOIN lowers to a keyless Cross; APPLY/natural/USING/non-equi stay rejected.
	assert!(
		lower_join_operator.contains("JoinOperator::CrossJoin(JoinConstraint::None)"),
		"a bare CROSS JOIN must lower to JoinKind::Cross"
	);
	assert!(
		lower_join_operator.contains("JoinKind::Cross"),
		"lower_join_operator must produce JoinKind::Cross"
	);
	assert!(
		lower_join_operator.contains("JoinOperator::CrossApply"),
		"CROSS APPLY must stay in the reject arm"
	);

	// The separate comma-join fence (`from.len() > 1`) is lifted into a left-deep Cross fold.
	assert!(
		!planner.contains("comma joins are not supported"),
		"the comma-join reject must be lifted"
	);
	assert!(
		planner.contains("for table_with_joins in from.iter().skip(1)"),
		"comma joins must fold the trailing FROM entries"
	);
	assert!(
		planner.contains("build_auto_join_plan(plan, right, JoinKind::Cross, Vec::new())"),
		"the comma fold must produce a keyless Cross (on: [])"
	);
}

#[test]
pub(crate) fn adr239_cross_join_forces_broadcast_strategy() {
	let join = source("src/binder/join.rs");

	// D4: auto strategy for Cross is dispatched to a dedicated broadcast-forcing decision.
	let choose = source_block(&join, "fn choose_join_strategy");
	assert!(
		choose.contains("matches!(kind, JoinKind::Cross)")
			&& choose.contains("choose_cross_join_strategy"),
		"choose_join_strategy must route Cross to the broadcast-forcing decision"
	);
	let cross = source_block(&join, "fn choose_cross_join_strategy");
	assert!(
		cross.contains("JoinStrategy::Broadcast"),
		"Cross must resolve to JoinStrategy::Broadcast"
	);

	// Both broadcast Inner-only gates (binder + fragmenter) are retargeted to accept Cross.
	let safe_binder = source_block(&join, "fn safe_broadcast_physical_kind");
	assert!(
		safe_binder.contains("JoinKind::Cross"),
		"the binder broadcast shape guard must accept Cross"
	);
	let probe_linearity = source_block(&join, "fn broadcast_probe_linearity");
	assert!(
		probe_linearity.contains("matches!(kind, JoinKind::Inner | JoinKind::Cross)"),
		"broadcast probe-linearity must treat left-deep broadcast Cross like broadcast Inner"
	);
	let fragment = source("src/distributed/fragment.rs");
	let safe_fragment = source_block(&fragment, "fn safe_broadcast_physical_join_kind");
	assert!(
		safe_fragment.contains("JoinKind::Cross"),
		"the fragmenter broadcast guard must accept Cross"
	);
}

#[test]
pub(crate) fn adr239_cross_join_does_not_hash_partition_empty_keys() {
	let join = source("src/binder/join.rs");
	let cross = source_block(&join, "fn choose_cross_join_strategy");

	// D4: no hash fallback / empty-key hash; an ineligible build fails loud.
	assert!(
		cross.contains("cross_requires_broadcast_error"),
		"Cross must fail loud when no broadcast-eligible build exists"
	);
	assert!(
		!cross.contains("hash_join_decision"),
		"Cross must never fall back to a hash join decision"
	);
	let error = source_block(&join, "fn cross_requires_broadcast_error");
	assert!(
		error.contains("single bucket"),
		"the fail-loud error must explain the empty-key single-bucket trap"
	);
}

#[test]
pub(crate) fn adr239_cross_join_executor_match_all_path_is_explicit() {
	let exec = source("src/exec.rs");

	// D5: a dedicated keyless match-all path, routed before the hash build/probe.
	assert!(
		exec.contains("fn execute_cross_join_match_all"),
		"the executor must have an explicit Cross match-all path"
	);
	let execute_hash_join = source_block(&exec, "async fn execute_hash_join");
	assert!(
		execute_hash_join.contains("matches!(kind, JoinKind::Cross)")
			&& execute_hash_join.contains("execute_cross_join_match_all"),
		"Cross must route to the explicit match-all path before the hash build/probe"
	);
}

#[test]
pub(crate) fn adr239_cross_join_preserves_sql_column_order_under_broadcast() {
	let join = source("src/binder/join.rs");
	let placement = source_block(&join, "fn broadcast_physical_placement");

	// D6: deterministic SQL right-build via the existing swap+restore path (no bespoke no-swap path).
	assert!(
		placement.contains("JoinKind::Cross => BroadcastPlacementRule::SwappedRightBuild"),
		"Cross must use the SwappedRightBuild placement"
	);
	let binder = binder_source();
	assert!(
		binder.contains("restore_declared_join_order_project"),
		"the swap must be paired with the declared-order restoration projection"
	);

	let behavior = source("tests/cross_join_test.rs");
	assert!(
		behavior.contains("cross_join_preserves_sql_column_order_under_broadcast"),
		"the column-order behavior gate must exist"
	);
}

#[test]
pub(crate) fn adr239_distributed_cross_join_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");

	assert!(
		distributed.contains("distributed_cross_join_cartesian_is_value_discriminating"),
		"the distributed cross-join gate must exist"
	);
	let gate = source_block(
		&distributed,
		"async fn distributed_cross_join_cartesian_is_value_discriminating",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr239_no_optimizer_durable_wendelon_or_rune_surface() {
	// D8: Cross reuses the broadcast Join + the Join node — no optimizer-evidence, durable key,
	// system table, Wendelon change, or Rune-only surface. Wendelon (which owns durable schema and
	// the optimizer-fact producers) and the Rune engine must carry no Cross join surface.
	let wendelon_offenders: Vec<_> = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("JoinKind::Cross") || line.contains("CrossJoin"))
		.collect();
	assert!(
		wendelon_offenders.is_empty(),
		"CROSS must not cross into Wendelon (durable / optimizer-evidence surface): {wendelon_offenders:?}"
	);

	let rune = source("src/rune_engine.rs");
	assert!(
		!rune.contains("JoinKind::Cross") && !rune.contains("CrossJoin"),
		"CROSS must not add a Rune-only surface; it lowers via the shared SQL path"
	);
}
