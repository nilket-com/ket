//! ADR 240 — Multi-Dedup Distinct Aggregates architecture guards.
//!
//! The load-bearing proofs are the behavior gates in `group_by_test.rs` (mixed, multiple, shared
//! branch, AVG, computed, grouped-all-null, global Cross, join base) and the distributed cardinal in
//! `distributed_process_test.rs`. These guards pin the structural facts: a flagless planner-owned
//! fan-out + recombine, branch sharing by distinct-argument identity, null-safe grouped recombine
//! with no group-domain branch, global Cross recombine, and no new physical/optimizer/Wendelon/Rune
//! surface.

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
pub(crate) fn adr240_keeps_aggregate_function_flagless() {
	// D1: multi-dedup is a planner-owned desugar; no DISTINCT flag on the aggregate function or node.
	let ir = source("src/ir.rs");
	let aggregate_function = ir
		.split("pub enum AggregateFunction")
		.nth(1)
		.and_then(|rest| rest.split("#[derive").next())
		.expect("AggregateFunction enum");
	assert!(!aggregate_function.to_lowercase().contains("distinct"));
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn build_distinct_aggregate_rewrite"),
		"the multi-dedup rewrite is planner-owned"
	);
}

#[test]
pub(crate) fn adr240_multi_dedup_rewrites_to_branch_aggregate_recombine() {
	// D1: the rewrite fans out into branches and recombines into a normalized plan.
	let planner = source("src/sql_planner.rs");
	let rewrite = source_block(&planner, "fn build_distinct_aggregate_rewrite");
	let impl_block = source_block(&planner, "impl DistinctAggregateRewrite");

	assert!(rewrite.contains("RecombineBranch"));
	assert!(rewrite.contains("entry_slots"));
	assert!(impl_block.contains("fn into_aggregated_plan"));
	assert!(impl_block.contains("fn build_branch"));
	// build_branch composes Project -> Distinct -> Aggregate; recombine joins branches.
	assert!(impl_block.contains("LogicalPlan::Distinct"));
	assert!(impl_block.contains("LogicalPlan::Aggregate"));
}

#[test]
pub(crate) fn adr240_same_distinct_arg_shares_one_dedup_branch() {
	// D2: distinct aggregates over the same structural argument identity share one dedup branch.
	let planner = source("src/sql_planner.rs");
	let rewrite = source_block(&planner, "fn build_distinct_aggregate_rewrite");

	assert!(rewrite.contains("dedup_identities"));
	assert!(rewrite.contains("expr_structural_eq"));
	assert!(rewrite.contains("canonical_aggregate_signature_expr"));
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("same_distinct_argument_shares_one_dedup_branch"));
}

#[test]
pub(crate) fn adr240_computed_distinct_args_are_projected_before_dedup() {
	// D3: a computed distinct argument is accepted and projected ahead of the Distinct.
	let planner = source("src/sql_planner.rs");
	let arg = source_block(&planner, "fn distinct_aggregate_arg");
	// No raw-column restriction any more (computed expressions are allowed).
	assert!(!arg.contains("matches!(arg, Expr::Column(_))"));
	let impl_block = source_block(&planner, "impl DistinctAggregateRewrite");
	assert!(impl_block.contains("branch_dedup_arg_name"));
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("computed_distinct_argument_executes"));
}

#[test]
pub(crate) fn adr240_avg_distinct_is_avg_over_dedup() {
	// D5: AVG(DISTINCT) emits ordinary AggregateFunction::Avg over the deduped rows — no planner-owned
	// sum/count final projection, no new kernel.
	let planner = source("src/sql_planner.rs");
	let arg = source_block(&planner, "fn distinct_aggregate_arg");
	assert!(arg.contains("AggregateFunction::Avg"));
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("avg_distinct_uses_avg_over_dedup_and_null_semantics"));
}

#[test]
pub(crate) fn adr240_grouped_recombine_uses_null_safe_group_key_join_without_group_domain_branch() {
	// D6: grouped recombine is an ADR 235 null-safe group-key join; branches are only dedup branches
	// or plain aggregate branches — there is no synthetic group-domain branch (no internal empty-agg
	// Aggregate). The all-NULL-group behavior gate locks that the recombine domain is still complete.
	let planner = source("src/sql_planner.rs");
	let recombine = source_block(&planner, "fn recombine");
	assert!(recombine.contains("null_safe_keys: true"));
	assert!(recombine.contains("JoinKind::Inner"));
	let rewrite = source_block(&planner, "fn build_distinct_aggregate_rewrite");
	assert!(
		!rewrite.contains("group_domain") && !rewrite.contains("groups_branch"),
		"no dedicated group-domain branch is constructed"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("grouped_multi_dedup_all_null_distinct_column_keeps_group"));
}

#[test]
pub(crate) fn adr240_global_recombine_uses_cross_join() {
	// D7: the global recombine uses a keyless Cross (the ADR 239 scalar-local path), not a synthetic key.
	let planner = source("src/sql_planner.rs");
	let recombine = source_block(&planner, "fn recombine");
	assert!(recombine.contains("JoinKind::Cross"));
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("global_multi_dedup_recombines_with_cross_join"));
}

#[test]
pub(crate) fn adr240_multi_dedup_composes_over_join_base() {
	// D8: a distinct + plain aggregate over a JOIN base executes (each branch clones the lowered base).
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("multi_dedup_over_join_executes"));
}

#[test]
pub(crate) fn adr240_distributed_gate_is_value_discriminating() {
	let distributed = source("tests/distributed_process_test.rs");
	assert!(
		distributed.contains("multi_dedup_distinct_aggregate_distributed_merges_across_producers")
	);
	let gate = source_block(
		&distributed,
		"async fn multi_dedup_distinct_aggregate_distributed_merges_across_producers",
	);
	assert!(
		gate.contains("assert_cross_producer_cardinal_is_value_discriminating"),
		"the distributed gate must use the 236/238 cross-producer cardinal discipline"
	);
}

#[test]
pub(crate) fn adr240_no_new_physical_optimizer_durable_wendelon_or_rune_surface() {
	// D9: the rewrite reuses Project/Distinct/Aggregate/Join/Cross. No new ExecutionPlan operator and
	// no cross-ring/durable surface — the planner-only `DistinctAggregateRewrite` stays in the planner.
	let execution_plan = source_block(&source("src/ir/execution.rs"), "pub enum ExecutionPlan");
	assert!(!execution_plan.contains("MultiDedup"));
	assert!(!execution_plan.contains("DistinctAggregate"));
	for relative in [
		"src/distributed/fragment.rs",
		"src/rune_engine.rs",
		"src/wire.rs",
	] {
		assert!(
			!source(relative).contains("DistinctAggregateRewrite"),
			"DistinctAggregateRewrite must stay planner-internal: {relative}"
		);
	}
	let wendelon = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<String>();
	assert!(!wendelon.contains("DistinctAggregateRewrite"));
}
