//! ADR 255 — local `WITH RECURSIVE` (UNION ALL fixpoint) architecture guards.
//!
//! Load-bearing proofs are the behavior gates in `group_by_test.rs`. These guards pin the structural
//! facts: recursive CTEs have real logical + execution nodes, execute as a bounded local fixpoint that
//! injects the working table via `SystemTable` substitution, and stay local-only (the recursive term is
//! the exchange-free scan/filter/project shape; `UNION` distinct, joins-in-recursion, and distributed
//! recursion reject — deferred to the follow-on).

use super::support::*;

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

#[test]
pub(crate) fn adr255_recursive_cte_has_logical_and_execution_nodes() {
	let ir = source("src/ir.rs");
	let execution = source("src/ir/execution.rs");
	assert!(
		ir.contains("RecursiveCte {") && ir.contains("RecursiveRef {"),
		"LogicalPlan carries the recursive-CTE + self-reference nodes"
	);
	assert!(
		execution.contains("RecursiveCte {") && execution.contains("RecursiveRef {"),
		"ExecutionPlan carries the bound recursive-CTE + self-reference nodes"
	);
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn lower_recursive_cte"),
		"the WITH RECURSIVE lowering exists"
	);
	assert!(
		!planner.contains("WITH RECURSIVE is not supported"),
		"the 254 deferral reject is gone"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("recursive_cte_counter_series_enumerates")
			&& tests.contains("recursive_cte_outer_query_consumes_result"),
		"the enumerate + outer-consume discriminators are pinned"
	);
}

#[test]
pub(crate) fn adr255_recursive_cte_executes_a_bounded_fixpoint() {
	let exec = source("src/exec.rs");
	assert!(
		exec.contains("fn execute_recursive_cte"),
		"the fixpoint loop exists"
	);
	assert!(
		exec.contains("RECURSIVE_CTE_MAX_ITERATIONS"),
		"a safety cap bounds iteration (no hang)"
	);
	assert!(
		exec.contains("fn substitute_recursive_ref") && exec.contains("SystemTable"),
		"the working table is injected by substituting RecursiveRef with a SystemTable each iteration"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("recursive_cte_terminates_on_empty_recursive_term")
			&& tests.contains("recursive_cte_nonterminating_hits_iteration_cap"),
		"the termination + cap discriminators are pinned"
	);
}

#[test]
pub(crate) fn adr255_recursive_cte_is_local_only() {
	// The recursive term is restricted to the exchange-free scan/filter/project shape; richer forms and
	// distributed recursion are deferred.
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn recursive_term_shape_is_local"),
		"the first-cut local shape gate exists"
	);
	assert!(
		planner.contains("recursive UNION (distinct) is not supported; use UNION ALL"),
		"the distinct form is rejected (deferred with joins-in-recursion)"
	);
	let fragment = source("src/distributed/fragment.rs");
	assert!(
		!fragment.contains("fn fragment_recursive"),
		"a recursive CTE is an opaque single-task local node, not a distributed fragment"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("recursive_cte_union_distinct_and_malformed_shapes_reject"),
		"the reject discriminator is pinned"
	);
}
