use std::fs;
use std::path::{Path, PathBuf};

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{
	ProductBoundary, assert_product_dependency_boundary,
	assert_workspace_product_dependency_boundary,
};

fn collect_mod_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
	let entries = fs::read_dir(dir).expect("read_dir should succeed");
	for entry in entries {
		let entry = entry.expect("directory entry should load");
		let path = entry.path();
		if path.is_dir() {
			collect_mod_rs_files(&path, files);
		} else if path.file_name().and_then(|name| name.to_str()) == Some("mod.rs") {
			files.push(path);
		}
	}
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
	let entries = fs::read_dir(dir).expect("read_dir should succeed");
	for entry in entries {
		let entry = entry.expect("directory entry should load");
		let path = entry.path();
		if path.is_dir() {
			collect_rs_files(&path, files);
		} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
			files.push(path);
		}
	}
}

fn strip_line_comment(line: &str) -> &str {
	line.split_once("//").map(|(code, _)| code).unwrap_or(line)
}

fn is_allowed_manifest_line(line: &str) -> bool {
	line.is_empty()
		|| line.starts_with("mod ")
		|| line.starts_with("pub mod ")
		|| line.starts_with("pub(crate) mod ")
		|| line.starts_with("use ")
		|| line.starts_with("pub use ")
		|| line.starts_with("pub(crate) use ")
		|| line.starts_with("#[cfg")
		|| line.starts_with("#[allow")
		|| line.starts_with("#[doc")
}

fn contains_forbidden_keyword(line: &str) -> bool {
	line.split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
		.any(|token| {
			matches!(
				token,
				"struct" | "enum" | "fn" | "const" | "impl" | "static"
			)
		})
}

#[test]
fn test_mod_rs_files_are_manifests_only() {
	let mut mod_files = Vec::new();
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	collect_mod_rs_files(&manifest_dir.join("src"), &mut mod_files);
	mod_files.sort();

	let mut violations = Vec::new();

	for path in mod_files {
		let content = fs::read_to_string(&path).expect("mod.rs should be readable");
		let mut in_block_comment = false;
		let mut in_use_block = false;

		for (line_no, raw_line) in content.lines().enumerate() {
			let mut line = raw_line.trim();

			if in_block_comment {
				if let Some((_, rest)) = line.split_once("*/") {
					in_block_comment = false;
					line = rest.trim();
				} else {
					continue;
				}
			}

			if line.starts_with("/*") {
				if !line.contains("*/") {
					in_block_comment = true;
				}
				continue;
			}

			if matches!(line, "" | "}" | "{")
				|| line.starts_with("//")
				|| line.starts_with("///")
				|| line.starts_with("//!")
				|| line.starts_with('*')
			{
				continue;
			}

			line = strip_line_comment(line).trim();
			if line.is_empty() {
				continue;
			}

			if in_use_block {
				if line.ends_with(';') {
					in_use_block = false;
				}
				continue;
			}

			if is_allowed_manifest_line(line) {
				if (line.starts_with("use ")
					|| line.starts_with("pub use ")
					|| line.starts_with("pub(crate) use "))
					&& !line.ends_with(';')
				{
					in_use_block = true;
				}
				continue;
			}

			let reason = if contains_forbidden_keyword(line) {
				format!("forbidden item on line {}", line_no + 1)
			} else {
				format!("unexpected manifest content on line {}", line_no + 1)
			};
			violations.push(format!(
				"{}: {}: {}",
				path.display(),
				reason,
				raw_line.trim()
			));
		}
	}

	assert!(
		violations.is_empty(),
		"mod.rs files must be manifests only:\n{}",
		violations.join("\n")
	);
}

#[test]
fn semantic_none_is_the_only_neutral_kind_and_never_decides_liveness() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let crates_dir = manifest_dir.parent().expect("workspace crates directory");
	let mut rust_sources = Vec::new();
	collect_rs_files(crates_dir, &mut rust_sources);
	let legacy_variant = ["SemanticKind::", "Group"].concat();
	for path in rust_sources {
		let source = fs::read_to_string(&path).expect("Rust source should be readable");
		assert!(
			!source.contains(&legacy_variant),
			"{} reintroduced the topology name into the semantic algebra",
			path.display()
		);
	}

	let registry =
		fs::read_to_string(manifest_dir.join("src/ecs/registry.rs")).expect("registry source");
	let currency = registry
		.split("pub(crate) fn is_current_node")
		.nth(1)
		.and_then(|tail| tail.split("pub(crate) fn checked_node_index").next())
		.expect("NodeId currency function");
	assert!(
		currency.contains("slot_published") && currency.contains("object_generation"),
		"NodeId currency must remain publication- and generation-based"
	);
	assert!(
		!currency.contains("kinds"),
		"SemanticKind::None is an algebra value, never a liveness sentinel"
	);
}

#[test]
fn semantic_kind_source_carries_the_normative_vocabulary_contract() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let kind_source =
		fs::read_to_string(manifest_dir.join("src/uast/kind.rs")).expect("semantic kind source");
	let normalized_docs = kind_source
		.lines()
		.filter_map(|line| line.trim_start().strip_prefix("///"))
		.map(str::trim)
		.collect::<Vec<_>>()
		.join(" ");

	for required_contract in [
		"The identity element of semantic interpretation",
		"Establishes a semantic context containing addressable members or nested",
		"One addressable member of a semantic context",
		"Residual syntax not promoted to a table or row",
		"not guaranteed to be a topology leaf",
		"Consumers must determine leafness from topology (`first_child`), never from this semantic kind",
		"does not promise Codd set semantics",
	] {
		assert!(
			normalized_docs.contains(required_contract),
			"SemanticKind must retain ADR 0003's normative contract: {required_contract}"
		);
	}
}

#[test]
fn adr_0005_keeps_analytics_out_of_the_document_engine() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Libaryon,
	);
}

#[test]
fn product_0001_keeps_editor_crates_out_of_the_nilket_workspace() {
	let product_root = Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.and_then(Path::parent)
		.expect("libaryon must live at crates/libaryon");
	assert_workspace_product_dependency_boundary(
		&product_root.join("nilket"),
		ProductBoundary::NilketEngine,
	);
}

#[test]
fn product_0059_store_stays_capability_closed_and_later_consumers_are_explicit() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let store_path = manifest_dir.join("src/core/session_store.rs");
	let source = fs::read_to_string(&store_path).expect("read session store");
	let production = source
		.split("\n\t#[cfg(test)]\n\tmod tests")
		.next()
		.expect("session store production source");

	for required in [
		"libc::O_CLOEXEC",
		"libc::O_NOFOLLOW",
		"libc::LOCK_EX | libc::LOCK_NB",
		"libc::LOCK_SH | libc::LOCK_NB",
		"the lock must precede even the first CURRENT read",
	] {
		assert!(
			production.contains(required),
			"session lock lost {required}"
		);
	}

	let mut engine_sources = Vec::new();
	collect_rs_files(&manifest_dir.join("src/engine"), &mut engine_sources);
	for path in engine_sources {
		let engine = fs::read_to_string(&path).expect("read engine source");
		let engine = engine
			.split("\n#[cfg(test)]\nmod tests {")
			.next()
			.expect("production engine prefix");
		let names = |identifier: &str| {
			engine
				.split(|character: char| !(character.is_alphanumeric() || character == '_'))
				.any(|token| token == identifier)
		};
		assert!(
			!names("SessionStore"),
			"{} made store discovery or creation an engine concern",
			path.display()
		);
		if names("SavedSession") {
			let name = path.file_name().and_then(|name| name.to_str());
			assert!(
				matches!(
					name,
					Some("host.rs" | "session.rs" | "session_checkpoint.rs")
				),
				"{} consumed the saved-session capability outside PRODUCT-0060's explicit host, live binding, or checkpoint boundary",
				path.display()
			);
		}
	}
	let host = fs::read_to_string(manifest_dir.join("src/engine/host.rs")).expect("read host");
	assert!(host.contains("pub fn spawn(config: EngineConfig) -> Self"));
	assert!(host.contains("pub fn spawn_saved("));

	let polariton = manifest_dir
		.parent()
		.expect("workspace crates")
		.join("polariton/src");
	let mut polariton_sources = Vec::new();
	collect_rs_files(&polariton, &mut polariton_sources);
	for path in polariton_sources {
		let head = fs::read_to_string(&path).expect("read Polariton source");
		let production = head.split("\n#[cfg(test)]").next().unwrap_or(&head);
		let file = path.file_name().and_then(|name| name.to_str());
		if production.contains("SessionStore") {
			assert!(
				matches!(file, Some("main.rs" | "state.rs")),
				"{} consumed session-store authority outside PRODUCT-0063's saved surface or PRODUCT-0073's terminal discovery",
				path.display()
			);
		}
		if production.contains(".discover()") {
			assert_eq!(
				file,
				Some("main.rs"),
				"{} moved PRODUCT-0073's read-only discovery outside the terminal entrypoint",
				path.display()
			);
			let listing = production
				.split("fn list_saved_sessions() -> gui::Result")
				.nth(1)
				.expect("PRODUCT-0073 terminal discovery")
				.split("fn render_session_discovery")
				.next()
				.expect("bounded PRODUCT-0073 terminal discovery");
			assert!(listing.contains("SessionStore::platform_default()?.discover()?"));
			assert!(
				!listing.contains("create(") && !listing.contains("open_writable"),
				"PRODUCT-0073 terminal discovery must not acquire writable authority"
			);
		}
		if production.contains("SavedSession") {
			assert!(
				matches!(file, Some("editor.rs" | "state.rs")),
				"{} moved PRODUCT-0063's saved startup authority outside the GUI controller/editor host boundary",
				path.display()
			);
		}
		if production.contains("DocumentId") {
			assert!(
				matches!(file, Some("editor.rs" | "session_persistence.rs")),
				"{} consumed durable document identity outside PRODUCT-0062's host/schema boundary",
				path.display()
			);
		}
	}
}

#[test]
fn product_0061_layout_payload_is_head_neutral_and_application_meaning_free() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(manifest_dir.join("src/engine/layout_checkpoint.rs"))
		.expect("read logical-layout owner");
	let dto = source
		.split("pub(crate) struct LayoutCheckpoint")
		.nth(1)
		.expect("checkpoint DTO start")
		.split("enum CapturedNode")
		.next()
		.expect("checkpoint DTO prefix");
	for forbidden in [
		"BufferId",
		"PaneId",
		"ViewId",
		"PaneRect",
		"viewport_width",
		"viewport_height",
		"LocalViewIdentity",
		"ColorIdentity",
		"polariton",
	] {
		assert!(
			!dto.contains(forbidden),
			"PRODUCT-0061 durable DTO must not contain runtime/head/application identity {forbidden}"
		);
	}
	for required in [
		"document_id",
		"HostPanelCheckpoint",
		"HostPanelGroupCheckpoint",
		"Vec<u32>",
	] {
		assert!(
			dto.contains(required),
			"PRODUCT-0061 durable DTO lost head-neutral field {required}"
		);
	}
}

#[test]
fn product_0061_projected_census_is_exactly_three_identity_sets() {
	use libaryon::uast::SharedIdentityCensus;
	use std::collections::BTreeSet;

	let SharedIdentityCensus {
		documents,
		host_panels,
		host_panel_groups,
	} = SharedIdentityCensus::default();
	fn require_set<T: Ord>(_: &BTreeSet<T>) {}
	require_set(&documents);
	require_set(&host_panels);
	require_set(&host_panel_groups);
	assert!(documents.is_empty() && host_panels.is_empty() && host_panel_groups.is_empty());
}

#[test]
fn product_0074_divider_profile_is_runtime_geometry_not_saved_layout() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let session = fs::read_to_string(manifest_dir.join("src/engine/session.rs"))
		.expect("read session geometry");
	let render = fs::read_to_string(manifest_dir.join("src/engine/render.rs"))
		.expect("read divider projection");
	for (name, source) in [("session", &session), ("render", &render)] {
		assert!(
			!source.contains("SPLIT_DIVIDER_COLS") && !source.contains("SPLIT_DIVIDER_ROWS"),
			"PRODUCT-0074 {name} geometry must consume PaneDividerReservation, not fixed constants"
		);
	}
	let layout = fs::read_to_string(manifest_dir.join("src/engine/layout_checkpoint.rs"))
		.expect("read logical layout codec");
	assert!(
		!layout.contains("PaneDividerReservation") && !layout.contains("divider_reservation"),
		"PRODUCT-0074 runtime presentation state must not enter libaryon_layout"
	);
}

#[test]
fn product_0060_trybuild_is_a_dev_only_capability_oracle() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read manifest");
	let (production, development) = manifest
		.split_once("[dev-dependencies]")
		.expect("libaryon declares dev dependencies");
	assert!(
		!production.contains("trybuild"),
		"trybuild must not enter libaryon's production dependency graph"
	);
	assert!(
		development
			.lines()
			.any(|line| line.starts_with("trybuild = ")),
		"trybuild must remain the dev-only compiler oracle"
	);
}

#[test]
fn product_0024_theme_construction_remains_owned_by_its_defining_module() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let crates_dir = manifest_dir.parent().expect("workspace crates directory");
	let mut rust_sources = Vec::new();
	collect_rs_files(crates_dir, &mut rust_sources);

	let mut possible_literals = Vec::new();
	for path in rust_sources {
		let source = fs::read_to_string(&path).expect("Rust source should be readable");
		for (line_number, line) in source.lines().enumerate() {
			let mut remainder = line;
			while let Some(index) = remainder.find("Theme {") {
				let preceding = remainder[..index].chars().next_back();
				let standalone = preceding.is_none_or(|ch| !(ch.is_alphanumeric() || ch == '_'));
				if standalone
					&& !line.contains("struct Theme {")
					&& !line.contains("impl Theme {")
					&& !line.contains("-> Theme {")
					&& !line.contains("\"Theme {")
				{
					possible_literals.push(format!(
						"{}:{}:{line}",
						path.display(),
						line_number + 1
					));
				}
				remainder = &remainder[index + "Theme {".len()..];
			}
		}
	}

	assert!(
		possible_literals.is_empty(),
		"Theme struct literals bypass resolved palette construction:\n{}",
		possible_literals.join("\n")
	);

	let theme_source = fs::read_to_string(manifest_dir.join("src/engine/theme.rs"))
		.expect("theme source should be readable");
	assert!(theme_source.contains("pub syntax_colors: [Option<Rgb>; CATEGORY_COUNT]"));
	assert!(theme_source.contains("palette: ResolvedThemePalette"));
	assert!(!theme_source.contains("pub palette: ResolvedThemePalette"));
}
