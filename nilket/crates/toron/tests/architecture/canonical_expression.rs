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

#[test]
pub(crate) fn adr0572_certificate_bytes_come_from_the_canonical_encoder_alone() {
	let rune = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read the Rune frontend");
	let verb = rune
		.split("pub fn materialize(")
		.nth(1)
		.and_then(|tail| tail.split("\n\tpub fn ").next())
		.expect("materialize verb body");
	assert_eq!(
		verb.matches("encode_v1").count(),
		1,
		"the certificate has exactly one encoder, cited exactly once"
	);
	for forbidden in ["postcard", "serde_json", "bincode", "to_vec("] {
		assert!(
			!verb.contains(forbidden),
			"the materialize path gained a second serialization {forbidden}"
		);
	}
	assert_eq!(
		rune.matches("materialization_certificate: Some(").count(),
		1,
		"only the materialize verb authors an attachment"
	);

	let sql_planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read the SQL planner");
	assert!(
		sql_planner.contains("materialization_certificate: None")
			&& !sql_planner.contains("materialization_certificate: Some"),
		"SQL CTAS never carries a certificate"
	);
}
