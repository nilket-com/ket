use super::support::*;

#[test]
pub(crate) fn adr545_floor_is_a_closed_typed_scalar_path() {
	let ir = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source");
	let execution_ir = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let arithmetic = std::fs::read_to_string(manifest_dir().join("src/binder/arithmetic.rs"))
		.expect("read arithmetic binder source");
	let evaluator = std::fs::read_to_string(manifest_dir().join("src/exec/expr.rs"))
		.expect("read expression evaluator source");
	let pbc = std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read PBC source");

	assert!(ir.contains("Floor(Box<Expr>)"));
	assert!(execution_ir.contains("Floor {\n\t\tinput: BoundExprId"));
	assert!(planner.contains("SqlExpr::Floor { expr, field }"));
	assert!(planner.contains("CeilFloorKind::DateTimeField"));
	assert!(arithmetic.contains("pub(super) fn bind_floor"));
	for data_type in [
		"DataType::Int32",
		"DataType::Int64",
		"DataType::Float32",
		"DataType::Float64",
	] {
		assert!(arithmetic.contains(data_type), "FLOOR lost {data_type}");
	}
	assert!(arithmetic.contains("LogicalTypeId::is_naked"));
	assert!(evaluator.contains("fn evaluate_floor"));
	assert!(evaluator.contains("!value.is_finite()"));
	assert!(evaluator.contains("value.floor()"));
	assert!(pbc.contains("FLOOR is not PBC-eligible"));
	assert!(!ir.contains("Expr::Ceil"));
	assert!(!ir.contains("Expr::Round"));
}

#[test]
pub(crate) fn adr545_alias_first_bucketing_does_not_widen_computed_grouping() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let group_tests = std::fs::read_to_string(manifest_dir().join("tests/group_by_test.rs"))
		.expect("read group-by tests");
	let lower_group_by = brace_blocks_after_marker(&planner, "fn lower_group_by_key(expr:")
		.into_iter()
		.next()
		.expect("lower_group_by_key body")
		.1;

	assert!(lower_group_by.contains("GROUP BY keys must be simple column references"));
	assert!(
		group_tests.contains("floor_alias_first_bucketing_is_sparse_and_handles_negative_bins")
	);
	assert!(group_tests.contains("SELECT FLOOR(price), COUNT(*) FROM sales GROUP BY FLOOR(price)"));
}
