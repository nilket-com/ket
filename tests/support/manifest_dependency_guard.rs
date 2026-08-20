use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProductBoundary {
	Libaryon,
	Baryon,
	Polariton,
	NilketEngine,
	Boson,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DependencySpec {
	alias: String,
	package: Option<String>,
	workspace: bool,
}

#[derive(Default)]
struct ManifestDependencies {
	direct: BTreeMap<String, DependencySpec>,
	workspace: BTreeMap<String, DependencySpec>,
}

#[derive(Clone, Debug)]
enum Section {
	DirectList,
	WorkspaceList,
	DirectDetail(String),
	WorkspaceDetail(String),
	Other,
}

#[allow(dead_code)]
pub(crate) fn assert_product_dependency_boundary(manifest_dir: &Path, boundary: ProductBoundary) {
	let workspace_dir = manifest_dir
		.parent()
		.and_then(Path::parent)
		.expect("crate must live directly below the workspace crates directory");
	assert_product_dependency_boundary_in_workspace(manifest_dir, workspace_dir, boundary);
}

#[allow(dead_code)]
pub(crate) fn assert_workspace_product_dependency_boundary(
	workspace_dir: &Path,
	boundary: ProductBoundary,
) {
	let mut manifests = Vec::new();
	collect_manifests(workspace_dir, workspace_dir, &mut manifests);
	manifests.sort();
	for manifest in manifests {
		let Some(manifest_dir) = manifest.parent() else {
			continue;
		};
		assert_product_dependency_boundary_in_workspace(manifest_dir, workspace_dir, boundary);
	}
}

#[allow(dead_code)]
fn collect_manifests(root: &Path, current: &Path, manifests: &mut Vec<std::path::PathBuf>) {
	let entries =
		fs::read_dir(current).unwrap_or_else(|error| panic!("read {}: {error}", current.display()));
	for entry in entries {
		let entry = entry.expect("read manifest directory entry");
		let path = entry.path();
		if path.is_dir() {
			if matches!(
				path.file_name().and_then(|name| name.to_str()),
				Some(".git" | "target")
			) {
				continue;
			}
			collect_manifests(root, &path, manifests);
		} else if path.file_name().and_then(|name| name.to_str()) == Some("Cargo.toml")
			&& path != root.join("Cargo.toml")
		{
			manifests.push(path);
		}
	}
}

fn assert_product_dependency_boundary_in_workspace(
	manifest_dir: &Path,
	workspace_dir: &Path,
	boundary: ProductBoundary,
) {
	let crate_manifest_path = manifest_dir.join("Cargo.toml");
	let workspace_manifest_path = workspace_dir.join("Cargo.toml");
	let crate_manifest = fs::read_to_string(&crate_manifest_path)
		.unwrap_or_else(|error| panic!("read {}: {error}", crate_manifest_path.display()));
	let workspace_manifest = fs::read_to_string(&workspace_manifest_path)
		.unwrap_or_else(|error| panic!("read {}: {error}", workspace_manifest_path.display()));

	let violations = forbidden_dependencies(&crate_manifest, &workspace_manifest, boundary);
	assert!(
		violations.is_empty(),
		"PRODUCT-0001 and KET-0005 forbid boundary-crossing dependencies in {}: {}. \
		 Amend plans/product/adr/0001_one_repository.md and the applicable subsystem ADR first, then \
		 update the dependency-boundary denylist.",
		manifest_dir.display(),
		violations.join(", ")
	);
}

fn forbidden_dependencies(
	crate_manifest: &str,
	workspace_manifest: &str,
	boundary: ProductBoundary,
) -> Vec<String> {
	let local = parse_manifest_dependencies(crate_manifest);
	let workspace = parse_manifest_dependencies(workspace_manifest);
	let mut violations = Vec::new();

	for dependency in local.direct.values() {
		let resolved = if dependency.workspace {
			workspace
				.workspace
				.get(&dependency.alias)
				.and_then(|inherited| inherited.package.as_deref())
				.unwrap_or(&dependency.alias)
		} else {
			dependency.package.as_deref().unwrap_or(&dependency.alias)
		};
		if is_forbidden_package(resolved, boundary) {
			if resolved == dependency.alias {
				violations.push(resolved.to_owned());
			} else {
				violations.push(format!("{} (package = {resolved})", dependency.alias));
			}
		}
	}

	violations
}

fn is_forbidden_package(package: &str, boundary: ProductBoundary) -> bool {
	let package = package.to_ascii_lowercase();
	let analytics_or_connector = package.starts_with("arrow")
		|| package.starts_with("parquet")
		|| package.starts_with("polars")
		|| package.starts_with("datafusion")
		|| package.starts_with("duckdb")
		|| package.contains("sqlite")
		|| package.starts_with("libsql")
		|| package.starts_with("sqlx")
		|| package.contains("postgres")
		|| package.starts_with("mysql")
		|| package.starts_with("odbc")
		|| package.starts_with("connectorx")
		|| package.starts_with("gcloud")
		|| package.contains("bigquery")
		|| package.contains("snowflake");
	let nilket_engine = package == "nilket"
		|| package == "toron"
		|| (package.starts_with("toron-") && package != "toron-client")
		|| package == "wendelon"
		|| package.starts_with("wendelon-");
	let editor = matches!(
		package.as_str(),
		"libaryon" | "baryon" | "polariton" | "spectrometer"
	);

	match boundary {
		ProductBoundary::Libaryon => analytics_or_connector || nilket_engine || package == "boson",
		ProductBoundary::Baryon => analytics_or_connector || nilket_engine,
		ProductBoundary::Polariton => nilket_engine,
		ProductBoundary::NilketEngine => editor,
		ProductBoundary::Boson => {
			analytics_or_connector
				|| nilket_engine
				|| editor || matches!(
				package.as_str(),
				"rune"
					| "sqlparser" | "serde"
					| "serde_json" | "postcard"
					| "bincode" | "toml"
					| "ron" | "ciborium"
			)
		}
	}
}

fn parse_manifest_dependencies(manifest: &str) -> ManifestDependencies {
	let mut parsed = ManifestDependencies::default();
	let mut section = Section::Other;
	let mut pending = String::new();
	let mut pending_depth = 0_i32;

	for raw_line in manifest.lines() {
		let line = strip_toml_comment(raw_line).trim();
		if line.is_empty() {
			continue;
		}

		if !pending.is_empty() {
			pending.push(' ');
			pending.push_str(line);
			pending_depth += delimiter_delta(line);
			if pending_depth <= 0 {
				apply_list_entry(&section, &pending, &mut parsed);
				pending.clear();
				pending_depth = 0;
			}
			continue;
		}

		if let Some(header) = section_header(line) {
			section = classify_section(header);
			ensure_detail_entry(&section, &mut parsed);
			continue;
		}

		match section {
			Section::DirectList | Section::WorkspaceList => {
				let depth = delimiter_delta(line);
				if depth > 0 {
					pending.push_str(line);
					pending_depth = depth;
				} else {
					apply_list_entry(&section, line, &mut parsed);
				}
			}
			Section::DirectDetail(ref alias) => {
				apply_detail_field(&mut parsed.direct, alias, line);
			}
			Section::WorkspaceDetail(ref alias) => {
				apply_detail_field(&mut parsed.workspace, alias, line);
			}
			Section::Other => {}
		}
	}

	if !pending.is_empty() {
		apply_list_entry(&section, &pending, &mut parsed);
	}
	parsed
}

fn ensure_detail_entry(section: &Section, parsed: &mut ManifestDependencies) {
	match section {
		Section::DirectDetail(alias) => {
			dependency_entry(&mut parsed.direct, alias);
		}
		Section::WorkspaceDetail(alias) => {
			dependency_entry(&mut parsed.workspace, alias);
		}
		_ => {}
	}
}

fn apply_list_entry(section: &Section, statement: &str, parsed: &mut ManifestDependencies) {
	let Some((raw_key, value)) = split_assignment(statement) else {
		return;
	};
	let (alias, property) = dependency_key(raw_key);
	if alias.is_empty() {
		return;
	}
	let dependencies = match section {
		Section::DirectList => &mut parsed.direct,
		Section::WorkspaceList => &mut parsed.workspace,
		_ => return,
	};
	let dependency = dependency_entry(dependencies, &alias);
	match property.as_deref() {
		Some("package") => dependency.package = toml_string(value),
		Some("workspace") => dependency.workspace = toml_bool(value).unwrap_or(false),
		Some(_) => {}
		None => {
			dependency.package = inline_string_field(value, "package");
			dependency.workspace = inline_bool_field(value, "workspace").unwrap_or(false);
		}
	}
}

fn apply_detail_field(
	dependencies: &mut BTreeMap<String, DependencySpec>,
	alias: &str,
	statement: &str,
) {
	let Some((key, value)) = split_assignment(statement) else {
		return;
	};
	let dependency = dependency_entry(dependencies, alias);
	match unquote(key.trim()).as_str() {
		"package" => dependency.package = toml_string(value),
		"workspace" => dependency.workspace = toml_bool(value).unwrap_or(false),
		_ => {}
	}
}

fn dependency_entry<'a>(
	dependencies: &'a mut BTreeMap<String, DependencySpec>,
	alias: &str,
) -> &'a mut DependencySpec {
	dependencies
		.entry(alias.to_owned())
		.or_insert_with(|| DependencySpec {
			alias: alias.to_owned(),
			..DependencySpec::default()
		})
}

fn classify_section(header: &str) -> Section {
	if header == "workspace.dependencies" {
		return Section::WorkspaceList;
	}
	if let Some(alias) = header.strip_prefix("workspace.dependencies.") {
		return Section::WorkspaceDetail(unquote(alias));
	}
	for dependency_kind in ["dependencies", "dev-dependencies", "build-dependencies"] {
		if header == dependency_kind || target_list_section(header, dependency_kind) {
			return Section::DirectList;
		}
		if let Some(alias) = header.strip_prefix(&format!("{dependency_kind}.")) {
			return Section::DirectDetail(unquote(alias));
		}
		let marker = format!(".{dependency_kind}.");
		if header.starts_with("target.")
			&& let Some((_, alias)) = header.rsplit_once(&marker)
		{
			return Section::DirectDetail(unquote(alias));
		}
	}
	Section::Other
}

fn target_list_section(header: &str, dependency_kind: &str) -> bool {
	header.starts_with("target.") && header.ends_with(&format!(".{dependency_kind}"))
}

fn section_header(line: &str) -> Option<&str> {
	line.strip_prefix('[')?.strip_suffix(']').map(str::trim)
}

fn dependency_key(key: &str) -> (String, Option<String>) {
	let key = key.trim();
	let mut in_string = false;
	let mut quote = '\0';
	for (index, ch) in key.char_indices() {
		match ch {
			'\'' | '"' if !in_string => {
				in_string = true;
				quote = ch;
			}
			_ if in_string && ch == quote => in_string = false,
			'.' if !in_string => {
				return (
					unquote(key[..index].trim()),
					Some(unquote(key[index + 1..].trim())),
				);
			}
			_ => {}
		}
	}
	(unquote(key), None)
}

fn split_assignment(statement: &str) -> Option<(&str, &str)> {
	let mut quote = '\0';
	let mut escaped = false;
	for (index, ch) in statement.char_indices() {
		if quote != '\0' {
			if quote == '"' && ch == '\\' && !escaped {
				escaped = true;
				continue;
			}
			if ch == quote && !escaped {
				quote = '\0';
			}
			escaped = false;
			continue;
		}
		match ch {
			'\'' | '"' => quote = ch,
			'=' => return Some((&statement[..index], statement[index + 1..].trim())),
			_ => {}
		}
	}
	None
}

fn inline_string_field(value: &str, field: &str) -> Option<String> {
	inline_field(value, field).and_then(toml_string)
}

fn inline_bool_field(value: &str, field: &str) -> Option<bool> {
	inline_field(value, field).and_then(toml_bool)
}

fn inline_field<'a>(value: &'a str, field: &str) -> Option<&'a str> {
	let bytes = value.as_bytes();
	let field_bytes = field.as_bytes();
	let mut start = 0;
	while start + field_bytes.len() <= bytes.len() {
		let offset = value[start..].find(field)? + start;
		let before_ok = offset == 0 || !is_key_byte(bytes[offset - 1]);
		let after = offset + field_bytes.len();
		let after_ok = after == bytes.len() || !is_key_byte(bytes[after]);
		if before_ok && after_ok {
			let tail = value[after..].trim_start();
			if let Some(tail) = tail.strip_prefix('=') {
				return Some(tail.trim_start());
			}
		}
		start = after;
	}
	None
}

fn is_key_byte(byte: u8) -> bool {
	byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn toml_string(value: &str) -> Option<String> {
	let value = value.trim_start();
	let quote = value.chars().next()?;
	if !matches!(quote, '\'' | '"') {
		return None;
	}
	let tail = &value[quote.len_utf8()..];
	let end = tail.find(quote)?;
	Some(tail[..end].to_owned())
}

fn toml_bool(value: &str) -> Option<bool> {
	let value = value.trim_start();
	if value.starts_with("true") {
		Some(true)
	} else if value.starts_with("false") {
		Some(false)
	} else {
		None
	}
}

fn unquote(value: &str) -> String {
	let value = value.trim();
	if value.len() >= 2
		&& ((value.starts_with('"') && value.ends_with('"'))
			|| (value.starts_with('\'') && value.ends_with('\'')))
	{
		value[1..value.len() - 1].to_owned()
	} else {
		value.to_owned()
	}
}

fn strip_toml_comment(line: &str) -> &str {
	let mut quote = '\0';
	let mut escaped = false;
	for (index, ch) in line.char_indices() {
		if quote != '\0' {
			if quote == '"' && ch == '\\' && !escaped {
				escaped = true;
				continue;
			}
			if ch == quote && !escaped {
				quote = '\0';
			}
			escaped = false;
			continue;
		}
		match ch {
			'\'' | '"' => quote = ch,
			'#' => return &line[..index],
			_ => {}
		}
	}
	line
}

fn delimiter_delta(value: &str) -> i32 {
	let mut quote = '\0';
	let mut escaped = false;
	let mut delta = 0;
	for ch in value.chars() {
		if quote != '\0' {
			if quote == '"' && ch == '\\' && !escaped {
				escaped = true;
				continue;
			}
			if ch == quote && !escaped {
				quote = '\0';
			}
			escaped = false;
			continue;
		}
		match ch {
			'\'' | '"' => quote = ch,
			'{' | '[' => delta += 1,
			'}' | ']' => delta -= 1,
			_ => {}
		}
	}
	delta
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn catches_inline_package_rename_and_ignores_comments() {
		let manifest = r#"
			[dependencies]
			foo = { version = "1", package = "arrow-array" } # disguised
			text = "1 # not a comment"
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Libaryon),
			["foo (package = arrow-array)"]
		);
	}

	#[test]
	fn catches_workspace_inherited_package_rename() {
		let workspace = r#"
			[workspace.dependencies]
			columnar = { package = "arrow", version = "1" }
		"#;
		let manifest = r#"
			[dependencies]
			columnar.workspace = true
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, workspace, ProductBoundary::Libaryon),
			["columnar (package = arrow)"]
		);
	}

	#[test]
	fn scans_all_dependency_kinds_and_target_specific_tables() {
		let manifest = r#"
			[dev-dependencies]
			polars-core = "1"

			[build-dependencies.renamed]
			package = "datafusion"
			version = "1"

			[target.'cfg(unix)'.dependencies]
			db = {
				package = "tokio-postgres",
				version = "1"
			}

			[target.'cfg(windows)'.build-dependencies.mysql_alias]
			package = "mysql_async"
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Baryon),
			[
				"db (package = tokio-postgres)",
				"mysql_alias (package = mysql_async)",
				"polars-core",
				"renamed (package = datafusion)",
			]
		);
	}

	#[test]
	fn baryon_may_consume_the_shared_analytics_crate_but_libaryon_may_not() {
		let manifest = "[dependencies]\nboson = { path = \"../boson\" }";
		assert!(forbidden_dependencies(manifest, "", ProductBoundary::Baryon).is_empty());
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Libaryon),
			["boson"]
		);
	}

	#[test]
	fn ordinary_editor_dependencies_are_allowed() {
		let manifest = r#"
			[dependencies]
			libaryon = { path = "../libaryon" }
			ratatui.workspace = true
			serde = "1"
			sqlparser = "1"
		"#;
		assert!(forbidden_dependencies(manifest, "", ProductBoundary::Baryon).is_empty());
	}

	#[test]
	fn existing_and_planned_product_connectors_are_forbidden() {
		let manifest = r#"
            [dependencies]
            gcloud-bigquery = "1"
            parquet = "1"
            snowflake-api = "1"
            tokio-rusqlite = "1"
        "#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Baryon),
			[
				"gcloud-bigquery",
				"parquet",
				"snowflake-api",
				"tokio-rusqlite",
			]
		);
	}

	#[test]
	fn editor_heads_cannot_reach_nilket_engines_directly() {
		let manifest = r#"
			[dependencies]
			toron = { path = "../nilket/crates/toron" }
			toron-tpch = "1"
			wendelon = "1"
			nilket = "1"
			toron-client = "1"
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Polariton),
			["nilket", "toron", "toron-tpch", "wendelon"]
		);
	}

	#[test]
	fn nilket_engines_cannot_reach_editor_crates_but_may_consume_boson() {
		let manifest = r#"
			[dependencies]
			baryon = "1"
			boson = "1"
			libaryon = "1"
			polariton = "1"
			toron = "1"
			wendelon = "1"
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::NilketEngine),
			["baryon", "libaryon", "polariton"]
		);
	}

	#[test]
	fn boson_rejects_all_engine_editor_and_codec_dependencies() {
		let manifest = r#"
			[dependencies]
			arrow = "1"
			baryon = "1"
			libaryon = "1"
			polars = "1"
			rune = "1"
			serde = "1"
			sqlparser = "1"
			toron = "1"
			wendelon = "1"
		"#;
		assert_eq!(
			forbidden_dependencies(manifest, "", ProductBoundary::Boson),
			[
				"arrow",
				"baryon",
				"libaryon",
				"polars",
				"rune",
				"serde",
				"sqlparser",
				"toron",
				"wendelon",
			]
		);
	}
}
