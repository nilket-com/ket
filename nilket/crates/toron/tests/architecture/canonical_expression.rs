//! KET-0006 — canonical generated-relation expression boundary guards.

use super::support::*;

fn source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/canonical_expression.rs"))
		.expect("read canonical expression lowerer")
}

#[test]
pub(crate) fn ket0006_lowering_is_bound_range_only_and_never_a_private_serialization() {
	let source = source();

	assert!(source.contains("pub(crate) fn try_canonical_generated_relation"));
	assert!(source.contains("ExecutionPlan::Range"));
	assert!(source.contains("ExecutionPlan::Project"));
	assert!(source.contains("BoundExprProgram"));
	for forbidden in [
		"LogicalPlan::",
		"serde::",
		"serde_json",
		"sqlparser",
		"RuneDataFrame",
		"to_string()",
		"format!(",
	] {
		assert!(
			!source.contains(forbidden),
			"KET-0006 lowerer gained forbidden fallback {forbidden}"
		);
	}
}
