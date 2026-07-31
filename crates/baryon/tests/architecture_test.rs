use std::fs;
use std::path::{Path, PathBuf};

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

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
fn adr_0005_keeps_analytics_implementation_out_of_the_terminal_head() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Baryon,
	);
}
