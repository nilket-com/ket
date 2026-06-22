//! ADR 257 — scalar subqueries (provably-scalar / global-aggregate form) architecture guards.
//!
//! The load-bearing proofs are the behavior gates in `group_by_test.rs`. These guards pin the structural
//! facts: the binder broadcasts a one-side-scalar (`row_upper_bound <= 1`) `Cross` via
//! `ScalarBroadcastCross` (the both-scalar `ScalarLocalCross` path is unchanged); the frontend pulls a
//! scalar `SqlExpr::Subquery` up into a keyless `Cross` + a column reference (frontend-only, reusing the
//! `Cross`/broadcast executor — no new execution op). ADR 320 adds a separate post-aggregate HAVING
//! scalar recombine; non-aggregate / correlated / aggregate-projection scalar boundaries stay deferred.

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
pub(crate) fn adr257_one_side_scalar_cross_is_broadcast_eligible() {
	let join = source("src/binder/join.rs");
	let cross = source_block(&join, "fn choose_cross_join_strategy");

	// A provably-scalar (row_upper_bound <= 1) side is broadcast-eligible: when exactly one side is scalar
	// the binder broadcasts it via ScalarBroadcastCross — a 1-row branch cannot multiply partitions.
	assert!(
		cross.contains("plan_is_scalar(&right_input.facts)")
			&& cross.contains("JoinStrategyDecisionReason::ScalarBroadcastCross"),
		"choose_cross_join_strategy must broadcast a one-side-scalar Cross via ScalarBroadcastCross"
	);
	assert!(
		cross.contains("JoinStrategy::Broadcast"),
		"the one-side-scalar Cross must resolve to a broadcast strategy"
	);

	// The both-scalar ScalarLocalCross path (ADR 239) is unchanged and still gates first.
	assert!(
		cross.contains("cross_inputs_are_scalar")
			&& cross.contains("JoinStrategyDecisionReason::ScalarLocalCross"),
		"the both-scalar ScalarLocalCross path must remain (checked before the one-side-scalar broadcast)"
	);
	let scalar = source_block(&join, "fn plan_is_scalar");
	assert!(
		scalar.contains("row_upper_bound") && scalar.contains("RowCount::new(1)"),
		"scalar means provably at most one row (row_upper_bound <= 1)"
	);

	let behavior = source("tests/group_by_test.rs");
	assert!(
		behavior.contains("one_side_scalar_cross_broadcasts_the_scalar"),
		"the binder-extension behavior gate must exist"
	);
}

#[test]
pub(crate) fn adr257_scalar_subquery_pulls_up_into_cross() {
	let planner = source("src/sql_planner.rs");

	// The frontend pull-up: a scalar SqlExpr::Subquery lowers into a keyless Cross plus a reference to the
	// aliased subplan's single column. Built via the keyless build_auto_join_plan (NOT the SQL
	// JOIN-with-derived path, which trips the adr145 fence). ADR 260 renamed the fold to
	// `join_scalar_subplans` (it also handles the correlated `LeftOuter` case); the uncorrelated scalar
	// still folds as this keyless Cross.
	assert!(
		planner.contains("fn pull_up_scalar_subqueries"),
		"the planner must pull scalar subqueries up"
	);
	let fold = source_block(&planner, "fn join_scalar_subplans");
	assert!(
		fold.contains("build_auto_join_plan(plan, subplan, JoinKind::Cross, vec![])"),
		"the uncorrelated scalar subplan must fold onto the plan as a keyless Cross via build_auto_join_plan"
	);
	let pull = source_block(&planner, "fn pull_up_scalar_subqueries");
	assert!(
		pull.contains("SqlExpr::Subquery(query)") && pull.contains("scalar_value_reference"),
		"a scalar SqlExpr::Subquery must be replaced by a qualified column reference"
	);
	assert!(
		source_block(&planner, "fn scalar_value_reference").contains("SqlExpr::CompoundIdentifier"),
		"the scalar value reference is a qualified column (CompoundIdentifier)"
	);

	let behavior = source("tests/group_by_test.rs");
	for marker in [
		"scalar_aggregate_subquery_in_projection_attaches_value",
		"scalar_aggregate_subquery_in_where_filters",
	] {
		assert!(
			behavior.contains(marker),
			"missing scalar-subquery pull-up behavior gate {marker}"
		);
	}
}

#[test]
pub(crate) fn adr257_scalar_subquery_pull_up_is_frontend_lowering_only() {
	// The pull-up + recombination of CLASSIFIABLE scalars (257 uncorrelated, 260 qualified-correlated) live
	// only in the planner and reuse the existing Cross/broadcast executor. (ADR 282 adds a transient
	// `DeferredScalarSubquery` node + `bind_deferred_scalar_subquery` resolve for the one case the catalog-free
	// planner CANNOT classify — an unqualified correlated ref; that handoff is exempt from this guard, which
	// still pins that the pull-up function + `__scalar_` synthesis stay out of the IR/executor/fragmenter.)
	assert!(
		source("src/sql_planner.rs").contains("fn pull_up_scalar_subqueries"),
		"the scalar-subquery pull-up lives in the planner"
	);
	for path in [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/exec.rs",
		"src/distributed/fragment.rs",
	] {
		let src = source(path);
		assert!(
			!src.contains("pull_up_scalar_subqueries") && !src.contains("__scalar_"),
			"scalar-subquery lowering must stay in the planner: {path}"
		);
	}
}

#[test]
pub(crate) fn adr257_non_aggregate_and_correlated_scalar_stay_deferred() {
	let planner = source("src/sql_planner.rs");
	for reject in [
		"non-aggregate scalar subqueries are not supported",
		"correlated scalar subqueries are not supported",
		"scalar subqueries in GROUP BY/HAVING are not supported",
	] {
		assert!(
			planner.contains(reject),
			"the deferred scalar-subquery boundary `{reject}` must be rejected"
		);
	}
	let behavior = source("tests/group_by_test.rs");
	assert!(
		behavior.contains("non_aggregate_and_correlated_scalar_subqueries_stay_rejected")
			&& behavior.contains("having_scalar_deferred_shapes_stay_rejected"),
		"the deferred-boundary behavior gates must exist"
	);
}

#[test]
pub(crate) fn adr320_having_scalar_recombines_post_aggregate() {
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn pull_up_having_scalar_subqueries")
			&& planner.contains("fn join_having_scalar_subplans"),
		"HAVING scalar collection and recombine must stay explicit in the planner"
	);
	let recombine = source_block(&planner, "fn join_having_scalar_subplans");
	assert!(
		recombine.contains("JoinKind::Inner")
			&& recombine.contains("JoinStrategy::HashPartitioned")
			&& recombine.contains("__having_scalar_outer_key_0")
			&& recombine.contains("__having_scalar_inner_key_0"),
		"HAVING scalar recombine must use side-specific synthetic keys on a hash join: {recombine}"
	);
	assert!(
		recombine.contains("multiple scalar subqueries in HAVING are not supported"),
		"the ADR 320 vertical slice must stay limited to one HAVING scalar"
	);
	let behavior = source("tests/group_by_test.rs");
	assert!(
		behavior.contains("scalar_aggregate_subquery_in_having_filters"),
		"the positive HAVING scalar behavior gate must exist"
	);
}
