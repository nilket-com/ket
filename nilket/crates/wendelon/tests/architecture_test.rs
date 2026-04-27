use std::fs;
use std::path::{Path, PathBuf};

fn walk_dir(dir: &Path, callback: &mut impl FnMut(&Path)) {
	if let Ok(entries) = fs::read_dir(dir) {
		for entry in entries.flatten() {
			let path = entry.path();
			if path.is_dir() {
				walk_dir(&path, callback);
			} else {
				callback(&path);
			}
		}
	}
}

fn collect_mod_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
	walk_dir(dir, &mut |path| {
		if path.file_name().and_then(|name| name.to_str()) == Some("mod.rs") {
			files.push(path.to_path_buf());
		}
	});
}

fn strip_line_comment(line: &str) -> &str {
	line.split_once("//").map(|(code, _)| code).unwrap_or(line)
}

fn is_allowed_manifest_line(line: &str) -> bool {
	let line = line.trim();
	line.is_empty()
		|| line.starts_with("mod ")
		|| line.starts_with("pub mod ")
		|| line.starts_with("use ")
		|| line.starts_with("pub use ")
		|| line.starts_with("#[cfg")
		|| line.starts_with("#[allow")
}

fn contains_forbidden_keyword(line: &str) -> bool {
	line.split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
		.any(|token| {
			matches!(
				token,
				"struct" | "enum" | "fn" | "const" | "impl" | "static" | "trait"
			)
		})
}

#[test]
fn test_mod_rs_files_are_manifests_only() {
	let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut mod_files = Vec::new();
	collect_mod_rs_files(&src_dir, &mut mod_files);
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
				if (line.starts_with("use ") || line.starts_with("pub use "))
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
fn test_forbidden_word_count() {
	let forbidden_word = "plane";
	let max_allowed = 2;
	let mut total_occurrences = 0;

	let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

	walk_dir(&src_dir, &mut |path| {
		if path.extension().map_or(false, |ext| ext == "rs") {
			if let Ok(content) = fs::read_to_string(path) {
				total_occurrences += content.matches(forbidden_word).count();
			}
		}
	});

	assert!(
		total_occurrences <= max_allowed,
		"The word '{}' appeared {} times, which exceeds the limit of {}.",
		forbidden_word,
		total_occurrences,
		max_allowed
	);
}

// Per ADR 041 (Project-name decoupling) Tier-1 enforcement: former
// brand-leaked names of the predicate-bytecode protocol are forbidden in code
// surfaces. The `Wendelon[A-Z]` Tier-2 audit is a separate follow-up effort and
// is not enforced by this test.
#[test]
fn test_no_brand_leaked_pbc_names_in_code_surfaces() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let scan_roots = ["src", "tests", "benches", "examples", "crates", "ci"];
	let mut violations: Vec<String> = Vec::new();

	for root in scan_roots {
		let root_path = manifest_dir.join(root);
		if !root_path.exists() {
			continue;
		}
		walk_dir(&root_path, &mut |path| {
			if !path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_some_and(|ext| matches!(ext, "rs" | "sh"))
			{
				return;
			}
			if path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name == "architecture_test.rs")
			{
				return;
			}
			let Ok(content) = fs::read_to_string(path) else {
				return;
			};
			for needle in ["WPBC", "Wpbc", "wpbc"] {
				if content.contains(needle) {
					violations.push(format!(
						"{}: contains forbidden token '{}'",
						path.display(),
						needle
					));
				}
			}
		});
	}

	let old_pbc_module = manifest_dir.join("src/schema/wpbc.rs");
	if old_pbc_module.exists() {
		violations.push(format!(
			"{}: forbidden module path; rename to src/schema/pbc.rs",
			old_pbc_module.display()
		));
	}

	assert!(
		violations.is_empty(),
		"WPBC brand-leaked names are forbidden in code surfaces (ADR 041 Tier-1):\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr042_protocol_surfaces_stay_brand_clean_and_unsuffixed() {
	let rendered_metrics = wendelon::metrics::MetricsRegistry::default().render_prometheus();
	let mut violations: Vec<String> = Vec::new();
	for metric in prometheus_metric_names(&rendered_metrics) {
		if metric.starts_with("wendelon_agg")
			|| metric.starts_with("wendelon_pbc")
			|| metric.starts_with("wendelon_storage_multirow")
		{
			if metric.starts_with("wendelon_w") {
				violations.push(format!(
					"metric '{metric}' brand-prefixes the protocol segment"
				));
			}
			if metric.contains("_v1") || metric.contains("_v2") {
				violations.push(format!("metric '{metric}' carries a version suffix"));
			}
		}
	}

	for column in [
		wendelon::schema::STORAGE_ROW_HLC_COLUMN,
		wendelon::schema::STORAGE_ROW_KEY_ID_COLUMN,
		wendelon::schema::STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	] {
		if !column.starts_with("__row_") || !column.ends_with("__") {
			violations.push(format!("synthetic column '{column}' is not row-functional"));
		}
		if column.contains("wendelon") || column.contains("_v1") || column.contains("_v2") {
			violations.push(format!(
				"synthetic column '{column}' is brand-prefixed or version-suffixed"
			));
		}
	}

	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	forbidden_protocol_tokens_in_code(manifest_dir, &mut violations);

	assert!(
		violations.is_empty(),
		"ADR 042 protocol surfaces must stay brand-clean, unsuffixed, and free of deleted scalar-context opcodes:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr043_storage_compression_surfaces_are_centralized() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	for manifest in ["Cargo.toml", "crates/wendelon-client/Cargo.toml"] {
		let content = fs::read_to_string(manifest_dir.join(manifest)).unwrap();
		assert!(
			content.contains("ipc_compression"),
			"{manifest} must keep Arrow ipc_compression enabled"
		);
	}

	let storage_ipc = fs::read_to_string(manifest_dir.join("src/storage/ipc.rs")).unwrap();
	assert!(storage_ipc.contains("try_with_compression"));
	assert!(storage_ipc.contains("CompressionType::LZ4_FRAME"));
	assert!(storage_ipc.contains("StreamWriter::try_new_with_options"));

	let dump_slab_record =
		fs::read_to_string(manifest_dir.join("src/bin/dump_slab_record.rs")).unwrap();
	assert!(dump_slab_record.contains("record_batch_compression_policy_from_ipc"));
	assert!(dump_slab_record.contains("decode_batch"));
	assert!(dump_slab_record.contains("strip_storage_synthetic_columns"));
	assert!(dump_slab_record.contains("user_visible_preview"));

	let storage_manager = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	assert!(
		storage_manager.contains(
			"pub(crate) async fn refresh_storage_compression_uncompressed_resident_bytes"
		)
	);
	assert!(storage_manager.contains("expensive diagnostic refresh"));
	assert!(storage_manager.contains("Prometheus scrape path"));

	let stabilization_runbook = fs::read_to_string(
		manifest_dir.join("plans/ops/runbooks/adr043_storage_body_stabilization.md"),
	)
	.unwrap();
	for signal in [
		"storage_compression_ratio",
		"storage_compression_decode_micros",
		"storage_compression_encode_micros",
		"storage_compression_uncompressed_resident_bytes",
		"storage_compression_decode_error_total",
		"storage_compression_l2_record_body_bytes_fetched_total",
		"pbc_slab_stats_pruned_total",
		"pbc_slab_stats_consulted_total",
		"storage_multirow_point_read_amplification_bytes",
	] {
		assert!(
			stabilization_runbook.contains(signal),
			"ADR 043 stabilization runbook must mention {signal}"
		);
	}

	let storage_dir = manifest_dir.join("src/storage");
	let mut bare_stream_writers = Vec::new();
	walk_dir(&storage_dir, &mut |path| {
		if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
			return;
		}
		let content = fs::read_to_string(path).unwrap();
		for (line_no, line) in content.lines().enumerate() {
			if line.contains("StreamWriter::try_new(") {
				bare_stream_writers.push(format!("{}:{}", path.display(), line_no + 1));
			}
		}
	});
	assert!(
		bare_stream_writers.is_empty(),
		"storage record-body writers must route through src/storage/ipc.rs; bare StreamWriter::try_new at:\n{}",
		bare_stream_writers.join("\n")
	);

	let schema_table = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	assert!(schema_table.contains("pub fn encode_arrow_schema_ipc"));
	assert!(
		!schema_table.contains("try_with_compression(Some"),
		"descriptor schema IPC must stay uncompressed"
	);

	let rendered_metrics = wendelon::metrics::MetricsRegistry::default().render_prometheus();
	for segment in [
		"storage_compression_records_written_total",
		"storage_compression_plaintext_bytes_total",
		"storage_compression_encoded_bytes_total",
		"storage_compression_encode_micros",
		"storage_compression_decode_micros",
		"storage_compression_decompressed_bytes_total",
		"storage_compression_decode_error_total",
		"storage_compression_l2_record_body_bytes_fetched_total",
		"storage_compression_ratio",
		"storage_compression_uncompressed_resident_bytes",
	] {
		// Keep the namespace and segment split so this fixture does
		// not self-match the ADR 043 brand-prefix grep guard.
		let metric = format!("{}{}", "wendelon_", segment);
		assert!(
			rendered_metrics.contains(&metric),
			"missing ADR 043 metric {metric}"
		);
	}
}

fn prometheus_metric_names(rendered: &str) -> Vec<&str> {
	let mut names = Vec::new();
	for line in rendered.lines() {
		let line = line.trim();
		if line.is_empty() {
			continue;
		}
		if let Some(rest) = line.strip_prefix("# HELP ") {
			if let Some(name) = rest.split_whitespace().next() {
				names.push(name);
			}
			continue;
		}
		if let Some(rest) = line.strip_prefix("# TYPE ") {
			if let Some(name) = rest.split_whitespace().next() {
				names.push(name);
			}
			continue;
		}
		if line.starts_with('#') {
			continue;
		}
		if let Some(name) = line.split_whitespace().next() {
			names.push(name.split_once('{').map(|(name, _)| name).unwrap_or(name));
		}
	}
	names.sort_unstable();
	names.dedup();
	names
}

fn forbidden_protocol_tokens_in_code(manifest_dir: &Path, violations: &mut Vec<String>) {
	let scan_roots = ["src", "tests", "benches"];
	let forbidden = [
		concat!("LOAD", "_", "IDENTITY"),
		concat!("ROUTE", "_", "BY", "_", "LOCALITY"),
		concat!("WENDELON", "_", "PBC_REJECT"),
		concat!("WENDELON", "_", "AGG"),
		concat!("WENDELON", "_", "CSB"),
		concat!("WENDELON", "_", "MULTIROW"),
		concat!("Wendelon", "Pbc"),
		concat!("Wendelon", "Agg"),
		concat!("Wendelon", "Csb"),
		concat!("Wendelon", "MultiRow"),
		concat!("W", "AGG"),
		concat!("W", "CSB"),
		concat!("W", "MULTIROW"),
		concat!("__wendelon", "_row"),
	];

	for root in scan_roots {
		let root_path = manifest_dir.join(root);
		if !root_path.exists() {
			continue;
		}
		walk_dir(&root_path, &mut |path| {
			if !path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_some_and(|ext| matches!(ext, "rs" | "toml" | "sh"))
			{
				return;
			}
			if path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name == "architecture_test.rs")
			{
				return;
			}
			let Ok(content) = fs::read_to_string(path) else {
				return;
			};
			for needle in forbidden {
				if content.contains(needle) {
					violations.push(format!(
						"{}: contains forbidden ADR 042 protocol token '{}'",
						path.display(),
						needle
					));
				}
			}
		});
	}
}
