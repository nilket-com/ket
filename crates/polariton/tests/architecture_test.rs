use std::path::Path;

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

fn envelope_leaks(source: &str, is_envelope: bool) -> Vec<&'static str> {
	let mut findings = Vec::new();
	if !is_envelope {
		for (needle, label) in [
			(
				"ast::EmptyBlock",
				"terminal-expression parsing escaped the envelope module",
			),
			(
				"ENTRYPOINT_PREFIX",
				"view-source rewriting escaped the terminal-envelope module",
			),
		] {
			if source.contains(needle) {
				findings.push(label);
			}
		}
	}
	for (needle, label) in [
		("RuneDataFrame", "Toron relation type entered Polariton"),
		("LogicalPlan", "Toron private plan entered Polariton"),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

#[test]
fn product_0001_polariton_reaches_nilket_only_through_the_public_client() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Polariton,
	);
}

#[test]
fn product_0003_terminal_envelope_is_the_only_relation_view_transformation_site() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut findings = Vec::new();
	let mut pending = vec![root];
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(&path).expect("read Polariton source") {
			let entry = entry.expect("source entry");
			let path = entry.path();
			if path.is_dir() {
				pending.push(path);
				continue;
			}
			if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
				continue;
			}
			let source = std::fs::read_to_string(&path).expect("read source");
			let is_envelope = path.file_name().and_then(|name| name.to_str())
				== Some("rune_relation_envelope.rs");
			for finding in envelope_leaks(&source, is_envelope) {
				findings.push(format!("{}: {finding}", path.display()));
			}
		}
	}
	assert!(
		findings.is_empty(),
		"PRODUCT-0003 containment violation:\n{}",
		findings.join("\n")
	);
	assert_eq!(
		envelope_leaks(
			"let _: ast::EmptyBlock; fn leaked(_: LogicalPlan) {}",
			false
		),
		vec![
			"terminal-expression parsing escaped the envelope module",
			"Toron private plan entered Polariton"
		],
		"positive control: the containment assay must observe a synthetic violation"
	);
}
