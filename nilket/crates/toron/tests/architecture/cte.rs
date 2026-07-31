//! ADR 254 — Common Table Expressions (non-recursive `WITH`) architecture guards.
//!
//! Load-bearing proofs are the behavior gates in `group_by_test.rs`. These guards pin the structural
//! facts: CTEs resolve as a frontend desugar (a `name -> plan` scope built in `lower_query_scoped` and
//! consulted in `lower_table_factor` before the catalog), bodies lower via the full query path, the
//! desugar carries no IR/binder/executor/fragmenter surface, and `WITH RECURSIVE` stays deferred.

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
pub(crate) fn adr254_cte_resolves_in_lowering() {
	// `WITH` resolves to a name -> plan scope built in `lower_query_scoped` and consulted in
	// `lower_table_factor` before the catalog; the query-level `WITH is not supported` reject is gone.
	let planner = source("src/sql_planner.rs");
	assert!(
		planner.contains("type CteScope")
			&& planner.contains("fn lower_query_scoped")
			&& planner.contains("fn cte_scope_view"),
		"the CTE scope + scoped lowering live in the planner"
	);
	assert!(
		!planner.contains("WITH is not supported"),
		"the query-level WITH reject is gone"
	);
	let table_factor = source_block(&planner, "fn lower_table_factor");
	assert!(
		table_factor.contains("if let Some(cte_plan) = cte.get("),
		"lower_table_factor resolves a CTE reference before the catalog ScanNamed"
	);
	let tests = source("tests/group_by_test.rs");
	for gate in [
		"cte_resolves_like_inline_subquery",
		"cte_body_with_join_binds_and_executes",
		"multiple_ctes_later_references_earlier",
		"cte_referenced_in_a_join",
		"cte_shadows_catalog_table",
		"cte_column_alias_list_renames_output",
		"derived_table_join_body_executes",
	] {
		assert!(tests.contains(gate), "missing CTE behavior gate {gate}");
	}
}

#[test]
pub(crate) fn adr254_cte_is_frontend_only() {
	// The CTE desugar lives entirely in lowering: no IR / binder / executor / fragmenter surface.
	assert!(
		source("src/sql_planner.rs").contains("type CteScope"),
		"the CTE scope type lives in the planner"
	);
	for path in [
		"src/ir.rs",
		"src/binder.rs",
		"src/exec.rs",
		"src/distributed/fragment.rs",
	] {
		let src = source(path);
		assert!(
			!src.contains("CteScope")
				&& !src.contains("cte_scope_view")
				&& !src.contains("lower_query_scoped"),
			"CTE resolution must stay in lowering: {path}"
		);
	}
}

#[test]
pub(crate) fn adr254_recursive_cte_stays_deferred() {
	// ADR 255 lands local `UNION ALL` `WITH RECURSIVE`: the deferral reject is gone and the lowering
	// builds the new `RecursiveCte` node. (Reject-string absence is the architecture guard here.)
	let planner = source("src/sql_planner.rs");
	assert!(!planner.contains("WITH RECURSIVE is not supported"));
	assert!(planner.contains("lower_recursive_cte"));
}
