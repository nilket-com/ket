use std::fs;
use std::path::Path;

fn dependency_table_violations(manifest: &str) -> Vec<&str> {
	manifest
		.lines()
		.map(str::trim)
		.filter(|line| {
			matches!(
				*line,
				"[dependencies]" | "[dev-dependencies]" | "[build-dependencies]"
			) || (line.starts_with("[target.") && line.ends_with(".dependencies]"))
		})
		.collect()
}

fn route_authority_violations(source: &str) -> Vec<&'static str> {
	let mut violations = Vec::new();
	if source.matches("const ROUTE_SPECS:").count() != 1 {
		violations.push("route table is not singular");
	}
	if source.matches("enum DefaultTarget").count() != 1 {
		violations.push("default eligibility type is not singular");
	}
	if source.contains("leaf: \"toron\"") || source.contains("leaf: \"wendelon\"") {
		violations.push("expert roles bypass Nilket's role grammar");
	}
	violations
}

#[test]
fn product_0004_keeps_the_router_dependency_free() {
	let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
		.expect("read ket manifest");
	assert!(
		dependency_table_violations(&manifest).is_empty(),
		"PRODUCT-0004 keeps ket dependency-free; make a new PRODUCT record before widening it"
	);

	let synthetic = "[package]\nname = \"ket\"\n[dependencies]\nlibaryon = {}\n";
	assert_eq!(
		dependency_table_violations(synthetic),
		vec!["[dependencies]"]
	);
}

#[test]
fn product_0004_keeps_one_route_authority_and_nilket_owned_roles() {
	let source = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
		.expect("read router source");
	assert!(
		route_authority_violations(&source).is_empty(),
		"PRODUCT-0004 requires one route/default authority and Nilket-owned role grammar; make a new PRODUCT record before widening it"
	);

	let duplicate = format!("{source}\nconst ROUTE_SPECS: () = ();\n");
	assert_eq!(
		route_authority_violations(&duplicate),
		vec!["route table is not singular"]
	);
	let duplicate_default = format!("{source}\nenum DefaultTarget {{ Other }}\n");
	assert_eq!(
		route_authority_violations(&duplicate_default),
		vec!["default eligibility type is not singular"]
	);
	let bypass = source.replace(
		"route: Route::Toron,\n\t\tname: \"toron\",\n\t\tleaf: \"nilket\"",
		"route: Route::Toron,\n\t\tname: \"toron\",\n\t\tleaf: \"toron\"",
	);
	assert_eq!(
		route_authority_violations(&bypass),
		vec!["expert roles bypass Nilket's role grammar"]
	);
}
