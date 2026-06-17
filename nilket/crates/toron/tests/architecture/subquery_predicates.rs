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
pub(crate) fn adr256_correlated_in_and_scalar_subquery_stay_deferred() {
	// ADR 258 lands `NOT IN` (the reject is gone); correlated IN, scalar subqueries, and SELECT-without-
	// FROM remain deferred.
	let planner = source("src/sql_planner.rs");
	assert!(
		// ADR 258 shares the membership-subquery lowering between IN / NOT IN, so the correlated reject is
		// a `{context}`-parameterized format string ("correlated IN…" / "correlated NOT IN…" at runtime).
		planner.contains("correlated {context} is not supported"),
		"correlated IN is deferred"
	);
	assert!(
		planner.contains("SELECT without FROM is not supported"),
		"SELECT without FROM stays deferred (its own cut)"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(tests.contains("correlated_in_and_scalar_subquery_stay_rejected"));
}

#[test]
pub(crate) fn adr258_not_in_desugars_to_not_exists_anti_joins() {
	// `x NOT IN (sub)` ≡ `x IS NOT NULL AND NOT EXISTS(sub WHERE key IS NULL) AND
	// NOT EXISTS(sub WHERE key = x)`: a null-`x` filter + a keyless null-guard `LeftAnti` + a membership
	// `LeftAnti` — no null-aware-anti operator.
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("fn lower_not_in_subquery_join"),
		"`NOT IN (subquery)` has a dedicated null-aware lowering"
	);
	let body = brace_blocks_after_marker(&planner, "fn lower_not_in_subquery_join")
		.into_iter()
		.next()
		.expect("lower_not_in_subquery_join body")
		.1;
	assert!(
		body.contains("Expr::IsNotNull"),
		"the null-`x` filter drops NULL outer keys: {body}"
	);
	assert!(
		body.contains("Expr::IsNull"),
		"the keyless null-guard filters the subquery to its NULL keys: {body}"
	);
	let anti_joins = body.matches("JoinKind::LeftAnti").count();
	assert!(
		anti_joins >= 2,
		"NOT IN desugars to two anti-joins (null-guard + membership), got {anti_joins}: {body}"
	);
	assert!(
		!planner.contains("NOT IN (subquery) is not supported"),
		"the 256 NOT IN reject is lifted"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("not_in_subquery_excludes_members")
			&& tests.contains("not_in_with_null_in_subquery_returns_no_rows")
			&& tests.contains("not_in_lowers_to_anti_joins_with_null_guard_and_null_x_filter"),
		"the NOT IN discriminators are pinned"
	);
}

#[test]
pub(crate) fn adr258_not_in_is_frontend_lowering_only() {
	assert!(
		source("src/sql_planner.rs").contains("fn lower_not_in_subquery_join"),
		"the NOT IN desugar lives in the planner"
	);
	for path in [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/exec.rs",
		"src/distributed/fragment.rs",
	] {
		let src = source(path);
		assert!(
			!src.contains("lower_not_in_subquery_join")
				&& !src.contains("NotInSubquery")
				&& !src.contains("NullAwareAnti"),
			"NOT IN lowering — and the absence of any null-aware-anti operator — stays out of the \
			 IR/executor/fragmenter: {path}"
		);
	}
}

#[test]
pub(crate) fn adr258_cross_type_and_correlated_not_in_stay_deferred() {
	// Correlated NOT IN rejects at lowering ("correlated {context}…" with context = "NOT IN"); cross-type
	// membership keys reject at bind (the deferred coercion gap, same as correlated IN / scalar subqueries).
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("correlated {context} is not supported"),
		"correlated IN / NOT IN stays deferred"
	);
	let tests = source("tests/group_by_test.rs");
	assert!(
		tests.contains("cross_type_and_correlated_not_in_stay_rejected"),
		"the cross-type / correlated NOT IN deferral is pinned"
	);
}
