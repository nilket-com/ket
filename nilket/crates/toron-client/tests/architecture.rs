use std::collections::BTreeMap;
use std::path::PathBuf;

const FORBIDDEN: &[&str] = &[
	"arrow",
	"arrow-array",
	"arrow-schema",
	"baryon",
	"datafusion",
	"libaryon",
	"polars",
	"polariton",
	"toron",
	"wendelon",
];

#[test]
fn public_client_manifest_excludes_analytics_editors_and_server_internals() {
	let crate_manifest =
		std::fs::read_to_string(manifest_path("Cargo.toml")).expect("read toron-client manifest");
	let workspace_manifest = std::fs::read_to_string(manifest_path("../../Cargo.toml"))
		.expect("read nilket workspace manifest");
	let violations = forbidden_dependencies(&crate_manifest, Some(&workspace_manifest));
	assert!(
		violations.is_empty(),
		"TORON-532 keeps toron-client transport-only; forbidden dependencies: {violations:?}"
	);
}

#[test]
fn manifest_guard_detects_alias_target_and_workspace_evasions() {
	let workspace = r#"
[workspace.dependencies]
columnar = { package = "arrow", version = "58" }
"#;
	let specimen = r#"
[dependencies]
frame = { package = "polariton", path = "../polariton" }
columnar.workspace = true

[target.'cfg(unix)'.dev-dependencies]
query = { package = "datafusion", version = "49" }
"#;
	assert_eq!(
		forbidden_dependencies(specimen, Some(workspace)),
		vec![
			"columnar -> arrow".to_owned(),
			"frame -> polariton".to_owned(),
			"query -> datafusion".to_owned(),
		],
		"positive control: the TORON-532 dependency assay must visibly fire"
	);
}

#[test]
fn product_0003_public_protocol_exposes_no_private_rune_plan_vocabulary() {
	let src = manifest_path("src");
	let mut violations = Vec::new();
	for entry in std::fs::read_dir(src).expect("read toron-client source") {
		let path = entry.expect("source entry").path();
		if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
			continue;
		}
		let source = std::fs::read_to_string(&path).expect("read source");
		for marker in private_rune_plan_vocabulary(&source) {
			violations.push(format!("{}: {marker}", path.display()));
		}
	}
	assert!(
		violations.is_empty(),
		"PRODUCT-0003 keeps Toron's Rune plan private: {violations:?}"
	);
	assert_eq!(
		private_rune_plan_vocabulary("pub fn leak(_: LogicalPlan) {}"),
		vec!["LogicalPlan"],
		"positive control: the private-plan assay must visibly fire"
	);
}

fn private_rune_plan_vocabulary(source: &str) -> Vec<&'static str> {
	[
		"LogicalPlan",
		"RuneEngine",
		"FrontendError",
		"crate::rune_engine",
	]
	.into_iter()
	.filter(|marker| source.contains(marker))
	.collect()
}

fn manifest_path(relative: &str) -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn forbidden_dependencies(manifest: &str, workspace: Option<&str>) -> Vec<String> {
	let manifest: toml::Value = toml::from_str(manifest).expect("parse specimen manifest");
	let workspace_dependencies = workspace
		.map(|source| toml::from_str::<toml::Value>(source).expect("parse workspace manifest"))
		.and_then(|value| {
			value
				.get("workspace")?
				.get("dependencies")?
				.as_table()
				.cloned()
		})
		.unwrap_or_default();

	let mut dependencies = BTreeMap::new();
	collect_dependency_tables(&manifest, &workspace_dependencies, &mut dependencies);
	dependencies
		.into_iter()
		.filter(|(_, package)| forbidden(package))
		.map(|(alias, package)| format!("{alias} -> {package}"))
		.collect()
}

fn collect_dependency_tables(
	manifest: &toml::Value,
	workspace: &toml::value::Table,
	out: &mut BTreeMap<String, String>,
) {
	for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
		if let Some(table) = manifest.get(section).and_then(toml::Value::as_table) {
			collect_dependencies(table, workspace, out);
		}
	}
	if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
		for target in targets.values().filter_map(toml::Value::as_table) {
			for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
				if let Some(table) = target.get(section).and_then(toml::Value::as_table) {
					collect_dependencies(table, workspace, out);
				}
			}
		}
	}
}

fn collect_dependencies(
	table: &toml::value::Table,
	workspace: &toml::value::Table,
	out: &mut BTreeMap<String, String>,
) {
	for (alias, declaration) in table {
		let resolved = declaration
			.as_table()
			.filter(|entry| entry.get("workspace").and_then(toml::Value::as_bool) == Some(true))
			.and_then(|_| workspace.get(alias))
			.unwrap_or(declaration);
		let package = resolved
			.as_table()
			.and_then(|entry| entry.get("package"))
			.and_then(toml::Value::as_str)
			.unwrap_or(alias);
		out.insert(alias.clone(), package.to_owned());
	}
}

fn forbidden(package: &str) -> bool {
	FORBIDDEN.iter().any(|forbidden| {
		package == *forbidden || (*forbidden == "arrow" && package.starts_with("arrow-"))
	})
}
