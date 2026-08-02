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

fn product_0005_private_discovery_leaks(source: &str) -> Vec<&'static str> {
	let mut findings = Vec::new();
	for (needle, label) in [
		(
			"runtime.json",
			"private runtime-manifest path entered Polariton",
		),
		(
			"127.0.0.1:7742",
			"canonical local endpoint was duplicated in Polariton",
		),
		(
			"struct RuntimeManifest",
			"runtime schema was redefined in Polariton",
		),
		(
			"struct ProfileMarker",
			"profile schema was redefined in Polariton",
		),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

#[test]
fn product_0005_polariton_uses_only_public_local_workspace_discovery() {
	let manifest =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
			.expect("read Polariton manifest");
	assert!(
		manifest.contains("nilket-local"),
		"PRODUCT-0005 requires Polariton to consume the public discovery crate"
	);

	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut pending = vec![root];
	let mut findings = Vec::new();
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(&path).expect("read Polariton source") {
			let path = entry.expect("source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
				let source = std::fs::read_to_string(&path).expect("read Polariton source file");
				for finding in product_0005_private_discovery_leaks(&source) {
					findings.push(format!("{}: {finding}", path.display()));
				}
			}
		}
	}
	assert!(
		findings.is_empty(),
		"PRODUCT-0005 discovery authority violation:\n{}",
		findings.join("\n")
	);
	assert_eq!(
		product_0005_private_discovery_leaks(
			"struct RuntimeManifest; const ENDPOINT: &str = \"127.0.0.1:7742\";"
		),
		vec![
			"canonical local endpoint was duplicated in Polariton",
			"runtime schema was redefined in Polariton",
		],
		"positive control: PRODUCT-0005 guard must observe private schema and endpoint copies"
	);
}
