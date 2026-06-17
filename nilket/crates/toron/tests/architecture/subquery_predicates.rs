//! ADR 256 — uncorrelated subquery predicates (`IN` / `EXISTS` / `NOT EXISTS`) architecture guards.
//!
//! Load-bearing proofs are the behavior gates in `group_by_test.rs`. These guards pin the structural
//! facts: `IN` / uncorrelated `EXISTS` / `NOT EXISTS` lower to the existing `LeftSemi`/`LeftAnti` kernels
//! in the WHERE clause (frontend-only), and `NOT IN` / correlated `IN` / scalar subqueries / `SELECT`
//! without `FROM` stay deferred.

use super::support::*;

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative))
		.unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

#[test]
pub(crate) fn adr256_in_and_uncorrelated_exists_reuse_semi_anti_join() {
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn lower_in_subquery_join"),
		"`IN (subquery)` lowers via a dedicated WHERE-clause join builder"
	);
	assert!(
		planner.contains("SqlExpr::InSubquery"),
		"`IN (subquery)` is handled in lower_where"
	);
	assert!(
		!planner.contains("uncorrelated EXISTS is not supported"),
		"the uncorrelated EXISTS reject is lifted (keyless semi/anti join)"
	);
	assert!(
		planner.contains("JoinKind::LeftSemi") && planner.contains("JoinKind::LeftAnti"),
		"reuse the landed semi/anti-join kernels — no new join machinery"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("in_subquery_lowers_to_semi_join")
			&& tests.contains("uncorrelated_exists_passes_all_rows_iff_subquery_nonempty")
			&& tests.contains("not_exists_lowers_to_anti_join"),
		"the IN / EXISTS / NOT EXISTS discriminators are pinned"
	);
}

#[test]
pub(crate) fn adr256_subquery_predicates_are_frontend_lowering_only() {
	assert!(
		source("src/sql_planner.rs").contains("fn lower_in_subquery_join"),
		"the IN/EXISTS desugar lives in the planner"
	);
	for path in [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/exec.rs",
		"src/distributed/fragment.rs",
	] {
		let src = source(path);
		assert!(
			!src.contains("lower_in_subquery_join") && !src.contains("InSubquery"),
			"subquery-predicate lowering must stay in the planner: {path}"
		);
	}
}

#[test]
pub(crate) fn adr256_not_in_and_scalar_subquery_stay_deferred() {
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("NOT IN (subquery) is not supported"),
		"NOT IN (null-aware) is deferred"
	);
	assert!(
		planner.contains("correlated IN is not supported"),
		"correlated IN is deferred"
	);
	assert!(
		planner.contains("SELECT without FROM is not supported"),
		"SELECT without FROM stays deferred (its own cut)"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("not_in_and_scalar_subquery_stay_rejected"));
}
