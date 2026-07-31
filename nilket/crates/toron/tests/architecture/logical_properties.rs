use super::support::*;

fn source_block(source: &str, marker: &str) -> String {
	source
		.split(marker)
		.nth(1)
		.and_then(|rest| rest.split("\n}\n").next())
		.unwrap_or_else(|| panic!("missing source block {marker}"))
		.to_string()
}

#[test]
pub(crate) fn adr520_unique_key_proofs_are_proof_grade_and_planner_dark() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let provenance = source_block(&catalog, "pub enum UniqueKeyProofProvenance");
	let proof = source_block(&catalog, "pub struct UniqueKeyProof");
	let binding = source_block(&catalog, "pub struct TableBinding");
	let binder = binder_source();
	let outer_count =
		std::fs::read_to_string(manifest_dir().join("src/binder/outer_count_preaggregation.rs"))
			.expect("read outer-count preaggregation source");
	assert_eq!(
		binder.matches("unique_key_proof").count(),
		outer_count.matches("unique_key_proof").count(),
		"ADR 521 is the binder's only proof consumer"
	);
	let binder_without_outer_count = binder.replace(&outer_count, "");
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read executor source");
	let wendelon_table =
		std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
			.expect("read Wendelon table schema");

	assert!(provenance.contains("EnforcedConstraint"));
	assert!(provenance.contains("VerifiedSnapshot"));
	assert!(!provenance.contains("Estimated"));
	assert!(!provenance.contains("Ndv"));
	assert!(proof.contains("columns: Vec<ColumnOrdinal>"));
	assert!(proof.contains("provenance: UniqueKeyProofProvenance"));
	assert!(binding.contains("unique_key_evidence: Vec<UniqueKeyProof>"));
	assert!(!binding.contains("pub unique_key_evidence"));
	for source in [&binder_without_outer_count, &planner, &exec] {
		assert!(
			!source.contains("unique_key_proof"),
			"ADR 521 confines proof consumption to its declining recognizer"
		);
	}
	assert!(outer_count.contains("binding.unique_key_proof(&[ordinal])"));
	assert!(!outer_count.contains("primary_key_columns"));
	assert!(!outer_count.contains("DistinctCountFact"));
	assert!(!outer_count.contains("estimated_ndv"));
	assert!(!wendelon_table.contains("UniqueKeyProof"));
	assert!(!wendelon_table.contains("unique_key_evidence"));
}
