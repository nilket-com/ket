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
fn test_adr048_retention_policy_surface_stays_typed_and_brand_clean() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let scan_roots = ["src", "tests", "benches"];
	let mut violations: Vec<String> = Vec::new();
	let forbidden_needles = [
		"RETENTION_REJECTED_BAD_UNIT",
		"RETENTION_REJECTED_NEGATIVE",
		"RETENTION_REJECTED_ZERO",
		"RETENTION_REJECTED_STALE_GENERATION",
		"TABLE_RETENTION_REQ",
		"TABLE_RETENTION_RESP",
		"RETENTION_REQ",
		"RETENTION_RESP",
		"parse_retention",
		"retention_parser",
		"parse_retention_policy",
		"parse_table_retention",
		"ALTER TABLE",
		"RETENTION =",
		"WendelonRetention",
		"wendelon_retention",
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
			for needle in forbidden_needles {
				if content.contains(needle) {
					violations.push(format!("{}: contains '{}'", path.display(), needle));
				}
			}
		});
	}

	assert!(
		violations.is_empty(),
		"ADR 048 retention policy engine surface must stay typed and brand-clean:\n{}",
		violations.join("\n")
	);

	let lifecycle_source =
		fs::read_to_string(manifest_dir.join("src/schema/lifecycle.rs")).unwrap();
	assert!(
		lifecycle_source.contains("pub enum TableRetentionPolicy {\n\tInheritCluster,\n\tDisabled,\n\tFixedWindow(RetentionWindowMillis),\n}"),
		"TableRetentionPolicy must stay a three-variant typed enum"
	);
	assert!(
		!lifecycle_source.contains("at_generation"),
		"ADR 048 rejected at_generation retention snapshots"
	);

	let table_source = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	let descriptor_block = table_source
		.split_once("pub struct TableSchemaDescriptorV1 {")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.expect("TableSchemaDescriptorV1 block should be present");
	assert!(
		!descriptor_block.to_ascii_lowercase().contains("retention"),
		"TableSchemaDescriptorV1 must not carry retention policy fields"
	);

	let transport_source =
		fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let capability_block = transport_source
		.split_once("pub enum NodeCapability {")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.expect("NodeCapability block should be present");
	assert!(
		!capability_block.contains("Retention"),
		"ADR 048 must not add a retention node capability bit"
	);

	let raft_storage_source =
		fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	assert!(
		raft_storage_source.contains("pub table_lifecycle: TableLifecycleCatalog,"),
		"SmState must keep the table lifecycle catalog explicit"
	);
	assert!(
		!raft_storage_source.contains("#[serde(default)]\n\tpub table_lifecycle"),
		"ADR 048 forbids a serde default shim on retention state"
	);

	let state_digest_source =
		fs::read_to_string(manifest_dir.join("src/hash/state_digest.rs")).unwrap();
	assert_eq!(
		state_digest_source
			.matches("StateDigestDomain(b\"digest.")
			.count(),
		5,
		"ADR 048 must not add a new state-digest domain constant"
	);
	assert!(
		state_digest_source.contains("apply_retention_eviction_to_visible_digest"),
		"ADR 048 retention must reuse the visible-domain eviction helper"
	);
	assert!(
		!state_digest_source.contains("retention_gc_element"),
		"ADR 048 must not introduce a retention-specific digest element helper"
	);

	let data_server_source =
		fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	assert!(
		data_server_source.contains("aggregate_csb_retention_decision"),
		"ADR 048 aggregate CSB path must gate retention-active direct answers by slab row-HLC bounds"
	);
	assert!(
		!data_server_source
			.contains("if prepared.retention_floor.is_some() {\n\t\t\treturn Ok(None);\n\t\t}"),
		"ADR 048 aggregate CSB path must not disable every retention-active direct answer"
	);

	let runbook =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/table_retention.md")).unwrap();
	for signal in [
		"SetTableRetentionPolicy",
		"wendelon_storage_table_retention_policy_updates_total",
		"wendelon_storage_table_retention_read_floor_rejects_total",
		"table.retention_policy_updated",
	] {
		assert!(
			runbook.contains(signal),
			"ADR 048 runbook must mention {signal}"
		);
	}
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

#[test]
fn test_adr044_columnar_fetch_stays_read_side_and_provider_neutral() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let descriptor_source = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	for forbidden in [
		concat!("column", "_fetch", "_policy"),
		concat!("Column", "Fetch", "Policy"),
	] {
		if descriptor_source.contains(forbidden) {
			violations.push(format!(
				"src/schema/table.rs: descriptor surface contains forbidden token '{forbidden}'"
			));
		}
	}

	let scan_roots = ["src", "tests", "benches"];
	let provider_or_format_forbidden = [
		concat!("Parque", "t"),
		concat!("parque", "t"),
		concat!("OR", "C"),
		concat!("S3 Selec", "t"),
		concat!("SelectObjectConten", "t"),
	];
	let capability_forbidden = [
		concat!("Columnar", "L2Fetch"),
		concat!("L2", "Columnar", "Fetch", "Capability"),
		concat!("Node", "Capability::", "Column"),
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
				.is_some_and(|ext| matches!(ext, "rs" | "toml"))
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
			for forbidden in provider_or_format_forbidden
				.iter()
				.chain(capability_forbidden.iter())
				.copied()
			{
				if content.contains(forbidden) {
					violations.push(format!(
						"{}: contains forbidden ADR 044 token '{}'",
						path.display(),
						forbidden
					));
				}
			}
		});
	}

	let columnar = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	if columnar.contains("AuthFailed") || columnar.contains("AuthenticationFailed") {
		violations.push(
			"src/storage/columnar.rs: auth failures must not be L2 fetch-strategy fallbacks"
				.to_string(),
		);
	}

	assert!(
		violations.is_empty(),
		"ADR 044 columnar fetch must stay read-side, provider-neutral, and auth-strict:\n{}",
		violations.join("\n")
	);
}

/// ADR 045 architectural guards.
///
/// Enforce that:
/// - The compactor / sweeper / multi-row recompaction sources never
///   re-call `synthesize_append_only_row_id`. Identities for surviving
///   rows are preserved byte-for-byte from the slab body.
/// - The retracted V1 counter design (`TableAppendState`,
///   `AppendOrdinal`, `reserve_append_ordinals`,
///   `__row_append_ordinal__`) does not reappear under a different
///   name.
/// - No descriptor coexistence shim
///   (`Option<PrimaryKeyDescriptor>` field) survives the Phase 1
///   rewrite.
/// - No node capability bit is added for append-only.
/// - `canonical_row_payload_hash` does not reappear in `src/`. Phase 7
///   resolved Open question 1 by dropping the canonical row payload
///   hash from the identity preimage on bench evidence; the helper
///   was deleted and must not be re-introduced.
/// - Append-only admission must not derive `WriterNodeId` from
///   `meta.proxy_source`. Phase 8 binds it from the admitting
///   server's authenticated `self.local_node`; the wire field
///   `proxy_source` is zeroed on direct SDK writes and is not
///   the authenticated-admitter contract.
/// - The `storage_append_only_identity_synthesis_micros` histogram
///   was deleted in Phase 8. Bench is the measurement surface for
///   per-row identity CPU.
/// - AppendOnly descriptor isolation: the matchers
///   (`table_record_matches_descriptor`,
///   `descriptor_matches_tiered_row`) accept a
///   `stored_table_object_id` argument and verify it for
///   AppendOnly. The schema-only matcher Phase 8 first shipped
///   was insufficient — same-tenant table-spread prefix
///   collisions could leak rows. Re-introducing
///   `RowIdentityPolicy::AppendOnly => Ok(true)` (no
///   `stored_table_object_id` check) regresses to that bug.
#[test]
fn test_adr045_append_only_invariants() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	// Compactor source must not call the append-only identity helper.
	let compactor_paths = [
		"src/storage/sweeper.rs",
		"src/storage/multirow.rs",
		"src/storage/manager.rs",
	];
	for relative in compactor_paths {
		let path = manifest_dir.join(relative);
		let content = match fs::read_to_string(&path) {
			Ok(content) => content,
			Err(_) => continue,
		};
		if content.contains("synthesize_append_only_row_id") {
			violations.push(format!(
				"{relative}: compactor / multirow source must not synthesize append-only \
				identities for existing rows"
			));
		}
	}

	// Retracted V1 counter symbols must not appear anywhere.
	let scan_roots = ["src", "tests", "benches"];
	let retracted_symbols = [
		"TableAppendState",
		"AppendOrdinal",
		"reserve_append_ordinals",
		"__row_append_ordinal__",
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
				.is_some_and(|ext| matches!(ext, "rs" | "toml"))
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
			for symbol in retracted_symbols {
				if content.contains(symbol) {
					violations.push(format!(
						"{}: contains retracted ADR 045 V1 counter symbol '{symbol}'",
						path.display()
					));
				}
			}
			// Coexistence-shim guard.
			if content.contains("Option<PrimaryKeyDescriptor") {
				violations.push(format!(
					"{}: contains Option<PrimaryKeyDescriptor> coexistence shim \
					 (forbidden by no-premature-backward-compat.md)",
					path.display()
				));
			}
			// Node capability bit guard.
			if content.contains("NodeCapability::AppendOnly")
				|| content.contains("AppendOnlyCapability")
			{
				violations.push(format!(
					"{}: append-only must not introduce a node capability bit; mixed PK \
					 and append-only fleets are descriptor-driven",
					path.display()
				));
			}
			// Phase 7 deletion guard. The canonical row payload hash
			// was dropped from the ADR 045 §4 preimage on Phase 7 bench
			// evidence (Open question 1, resolved). The helper must not
			// re-appear under any name.
			if content.contains("canonical_row_payload_hash") {
				violations.push(format!(
					"{}: `canonical_row_payload_hash` was deleted in Phase 7 — append-only \
					 admission must not reintroduce per-row payload hashing (ADR 045 §4)",
					path.display()
				));
			}
			// Phase 8 deletion guard. The
			// `storage_append_only_identity_synthesis_micros` histogram
			// was removed; bench is the measurement surface. Re-adding
			// the always-on per-row timer would re-introduce the cost
			// for a metric that is only useful in bench-grade
			// comparisons.
			if content.contains("storage_append_only_identity_synthesis_micros")
				|| content.contains("observe_storage_append_only_identity_synthesis")
			{
				violations.push(format!(
					"{}: append-only identity-synthesis histogram was deleted in Phase 8 — \
					 use the `benches/append_only_tables.rs` bench instead of an always-on \
					 production timer (ADR 045 Phase 8)",
					path.display()
				));
			}
			// Phase 8 writer-identity guard. Append-only admission
			// must bind `WriterNodeId` from the admitting server's
			// authenticated `self.local_node`, not from the wire
			// field `meta.proxy_source` (which the wire contract
			// zeros on direct SDK writes).
			if content.contains("WriterNodeId::from_node_id(meta.proxy_source)") {
				violations.push(format!(
					"{}: append-only admission must bind WriterNodeId from the admitting \
					 server's authenticated `self.local_node`, not `meta.proxy_source` \
					 (ADR 045 §3 / Phase 8). `proxy_source` is zeroed on direct SDK writes.",
					path.display()
				));
			}
			// Phase 8 descriptor-isolation guard for AppendOnly
			// matchers. The schema-only AppendOnly arm Phase 8
			// first shipped allowed cross-table prefix collisions
			// to leak rows; the fix is to verify
			// `stored_table_object_id == Some(descriptor.table_object_id)`.
			// Re-introducing the schema-only acceptance regresses
			// to that bug.
			let is_matcher_file = path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name == "data_server.rs" || name == "manager.rs");
			if is_matcher_file
				&& content.contains("RowIdentityPolicy::AppendOnly => {")
				&& !content.contains("descriptor.table_object_id")
			{
				violations.push(format!(
					"{}: AppendOnly matcher arm must verify stored_table_object_id == \
					 Some(descriptor.table_object_id) — schema-only acceptance permits \
					 cross-table prefix-collision leakage (ADR 045 §5 / Phase 8).",
					path.display()
				));
			}
			// ADR 045 idempotency-bytes canonicalization. Any
			// retry-stable derivation off `ClientWriteId` (locality
			// salts, content hashes, redb keys, parent identities,
			// etc.) MUST hash only the canonical idempotency
			// identity (`client_uuid` + `sequence_no`), via
			// `ClientWriteId::canonical_idempotency_bytes()`.
			// Hashing the full 32-byte struct also folds in
			// `idempotency_ttl_ms` (a server-clamped hint) and
			// `_pad` (alignment), which break retry convergence
			// when a client re-sends with a drifted TTL after the
			// idempotency cache eviction or a node restart. The
			// previously narrow guard (scoped to the
			// `"table.append-locality"` domain) was widened in the
			// Phase 8 idempotency-canonicalization pass once the
			// three pre-existing sites
			// (`PendingFinalizationId::for_record`,
			// `encode_write_id_key`,
			// `table_write_parent_identity`) were canonicalized.
			let forbidden_write_id_byte_patterns = [
				"bytemuck::bytes_of(&parent_write_id)",
				"bytemuck::bytes_of(&meta.parent_write_id)",
				"bytemuck::bytes_of(&write_id)",
				"bytemuck::bytes_of(&record.write_id)",
				"bytemuck::bytes_of(&meta.client_write_id)",
				"bytemuck::bytes_of(&self.write_id)",
			];
			for pattern in forbidden_write_id_byte_patterns {
				if content.contains(pattern) {
					violations.push(format!(
						"{}: forbidden raw-bytes hashing of `ClientWriteId` ({pattern}). \
						 Retry-stable derivations must use \
						 `ClientWriteId::canonical_idempotency_bytes()`; the full 32-byte \
						 struct includes `idempotency_ttl_ms` (a server-clamped hint) and \
						 `_pad`, which break retry convergence.",
						path.display()
					));
				}
			}
		});
	}

	assert!(
		violations.is_empty(),
		"ADR 045 architectural invariants violated:\n{}",
		violations.join("\n")
	);
}

/// ADR 046 architectural guards.
///
/// StateDigest is a hash-layer substrate only. It must not leak curve
/// math into request admission, storage ingestion, network routing, or
/// any durable/wire surface before a consumer ADR owns that surface.
#[test]
fn test_adr046_state_digest_substrate_boundaries() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	for root in ["src/network", "src/storage"] {
		let root_path = manifest_dir.join(root);
		if !root_path.exists() {
			continue;
		}
		walk_dir(&root_path, &mut |path| {
			if !path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_some_and(|ext| ext == "rs")
			{
				return;
			}
			let Ok(content) = fs::read_to_string(path) else {
				return;
			};
			if content.contains("curve25519_dalek") {
				violations.push(format!(
					"{}: curve25519-dalek must stay out of network/storage hot paths",
					path.display()
				));
			}
			if content.contains("StateDigest") {
				violations.push(format!(
					"{}: StateDigest must not be imported into storage/network durable or \
					 protocol surfaces in ADR 046",
					path.display()
				));
			}
		});
	}

	let src_dir = manifest_dir.join("src");
	walk_dir(&src_dir, &mut |path| {
		if !path
			.extension()
			.and_then(|ext| ext.to_str())
			.is_some_and(|ext| ext == "rs")
		{
			return;
		}
		let relative = path.strip_prefix(manifest_dir).unwrap_or(path);
		let Ok(content) = fs::read_to_string(path) else {
			return;
		};

		if content.contains("NodeCapability::StateDigest")
			|| content.contains("NodeCapability::Ecmh")
			|| content.contains("NodeCapability::ECMH")
			|| content.contains("StateDigestCapability")
			|| content.contains("EcmhCapability")
			|| content.contains("ECMHCapability")
		{
			violations.push(format!(
				"{}: ADR 046 must not add a node capability bit",
				relative.display()
			));
		}
		let version_suffix_tokens = [
			concat!("StateDigest", "V1"),
			concat!("StateDigest", "V2"),
			concat!("StateDigestDomain", "V1"),
			concat!("StateDigestDomain", "V2"),
			concat!("StateDigestAccumulator", "V1"),
			concat!("StateDigestAccumulator", "V2"),
			concat!("StateDigestElement", "V1"),
			concat!("StateDigestElement", "V2"),
		];
		if version_suffix_tokens
			.iter()
			.any(|token| content.contains(token))
		{
			violations.push(format!(
				"{}: ADR 046 Rust surfaces must not carry V1/V2 suffixes",
				relative.display()
			));
		}
		if content.contains("b\"table.append-row\"") && content.contains("b\"digest.table.row.") {
			violations.push(format!(
				"{}: append-row identity and state-digest row preimages must stay separated",
				relative.display()
			));
		}

		for (line_no, line) in content.lines().enumerate() {
			if !line.contains("StateDigestDomain(") {
				continue;
			}
			let trimmed = line.trim();
			let allowed = relative == Path::new("src/hash/state_digest.rs")
				&& (trimmed.starts_with("pub const DOMAIN_")
					|| trimmed.starts_with("StateDigestDomain(b\"digest.")
					|| trimmed.starts_with("pub struct StateDigestDomain(")
					|| trimmed.starts_with("const MALFORMED_DOMAIN_FOR_TEST:"));
			if !allowed {
				violations.push(format!(
					"{}:{}: StateDigestDomain construction must be a named module constant",
					relative.display(),
					line_no + 1
				));
			}
		}
	});

	assert!(
		violations.is_empty(),
		"ADR 046 architectural invariants violated:\n{}",
		violations.join("\n")
	);
}

/// ADR 047 architectural guards.
///
/// Row tombstones are now an approved append-only storage fact, but
/// the ADR intentionally keeps the surface narrow: shared
/// `__row_key_id__` identity, no target column, no capability bit,
/// no short footer alias, and no project-prefixed row-delete domain.
#[test]
fn test_adr047_row_tombstone_boundaries() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();
	// Scope this guard to source/test/bench code. Plans may quote
	// rejected peer-draft names while explaining ADR decisions.
	let scan_roots = ["src", "tests", "benches"];
	let forbidden = [
		concat!("__row_tombstone", "_target__"),
		concat!("has_", "tombstones"),
		concat!("NodeCapability::", "RowTombstone"),
		concat!("NodeCapability::", "Tombstone"),
		concat!("NodeCapability::", "DeleteRows"),
		concat!("RowTombstone", "Capability"),
		concat!("Tombstone", "Capability"),
		concat!("DeleteRows", "Capability"),
		concat!("wendelon.table", ".delete"),
		concat!("wendelon.table", "-delete"),
		concat!("TableDeleteRows", "V1"),
		concat!("TableDeleteRows", "V2"),
		concat!("DeleteRows", "V1"),
		concat!("DeleteRows", "V2"),
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
						"{}: contains forbidden ADR 047 row-tombstone token '{}'",
						path.display(),
						needle
					));
				}
			}
		});
	}

	let schema = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	if !schema.contains("STORAGE_ROW_TOMBSTONE_COLUMN") {
		violations
			.push("src/schema/table.rs: missing STORAGE_ROW_TOMBSTONE_COLUMN constant".to_string());
	}
	let crypto = fs::read_to_string(manifest_dir.join("src/crypto.rs")).unwrap();
	if !crypto.contains("HAS_ROW_TOMBSTONE_RECORDS_BIT")
		|| !crypto.contains("has_row_tombstone_records")
	{
		violations.push(
			"src/crypto.rs: missing ADR 047 has_row_tombstone_records footer bit plumbing"
				.to_string(),
		);
	}

	assert!(
		violations.is_empty(),
		"ADR 047 row-tombstone architectural invariants violated:\n{}",
		violations.join("\n")
	);
}

/// ADR 049 architectural guards.
///
/// Content-hash row identity is now an accepted table identity
/// policy, but its surface stays deliberately narrow: canonical
/// logical Arrow values only, no raw IPC preimage, no schema
/// generation/fingerprint in the row key, no new digest domain, and
/// no version-suffixed Rust identity types.
#[test]
fn test_adr049_content_hash_identity_boundaries() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let schema_source = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	if !schema_source.contains("AppendOnlyContentHash") {
		violations.push(
			"src/schema/table.rs: missing RowIdentityPolicy::AppendOnlyContentHash".to_string(),
		);
	}
	if !schema_source.contains("CONTENT_HASH_REJECTED_NON_NULL_DEFAULT")
		|| !schema_source.contains("CONTENT_HASH_REJECTED_DECIMAL_EVOLUTION")
		|| !schema_source.contains("CONTENT_HASH_REJECTED_INVALID_UTF8")
	{
		violations.push(
			"src/schema/table.rs: missing ADR 049 content-hash reject code surface".to_string(),
		);
	}
	if !schema_source.contains("AppendOnlyContentHash rejects LocalityPolicyV1::ColumnProjection") {
		violations.push(
			"src/schema/table.rs: content-hash ColumnProjection rejection must stay explicit"
				.to_string(),
		);
	}

	let data_server_source =
		fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	if !data_server_source.contains("bifurcate_content_hash_table_write")
		|| !data_server_source.contains("content_hash_admission_guard")
		|| !data_server_source.contains("observe_storage_content_hash_duplicate_absorption")
		|| !data_server_source.contains("row_visible_for_admission")
	{
		violations.push(
			"src/network/data_server.rs: missing explicit ADR 049 duplicate absorption path"
				.to_string(),
		);
	}
	let pk_delete_source =
		fs::read_to_string(manifest_dir.join("src/storage/pk_delete.rs")).unwrap();
	if !pk_delete_source.contains("row_visible_for_admission")
		|| !pk_delete_source.contains("row_visible_under_retention")
	{
		violations.push(
			"src/storage/pk_delete.rs: ADR 049 visible-membership helper must include ADR 048 retention visibility"
				.to_string(),
		);
	}

	let content_hash_path = manifest_dir.join("src/storage/content_hash.rs");
	let content_hash_source = fs::read_to_string(&content_hash_path).unwrap();
	let production_content_hash = content_hash_source
		.split("#[cfg(test)]")
		.next()
		.unwrap_or(&content_hash_source);
	if !production_content_hash.contains("DOMAIN_CONTENT_HASH_ROW")
		|| !production_content_hash.contains("b\"table.append-content-row.v1\"")
	{
		violations.push(
			"src/storage/content_hash.rs: missing exact ADR 049 row identity domain".to_string(),
		);
	}
	if !production_content_hash.contains("content_hash_row_key_id") {
		violations.push("src/storage/content_hash.rs: missing content-hash key helper".to_string());
	}
	for needle in [
		"arrow::ipc",
		"StreamWriter",
		"FileWriter",
		"IpcWriteOptions",
		"encode_arrow_schema_ipc",
		"schema_generation",
		"logical_schema_fingerprint",
		"physical_descriptor_fingerprint",
	] {
		if production_content_hash.contains(needle) {
			violations.push(format!(
				"src/storage/content_hash.rs: production encoder/preimage must not contain '{needle}'"
			));
		}
	}

	let scan_roots = ["src", "tests", "benches"];
	let forbidden = [
		concat!("AppendOnlyContentHash", "V1"),
		concat!("AppendOnlyContentHash", "V2"),
		concat!("ContentHashIdentity", "V1"),
		concat!("ContentHashIdentity", "V2"),
		concat!("digest.table.row.", "content.v1"),
		concat!("Wendelon", "ContentHash"),
		concat!("wendelon", "_content_hash"),
		concat!("NodeCapability::", "ContentHash"),
		concat!("ContentHash", "Capability"),
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
						"{}: contains forbidden ADR 049 content-hash token '{}'",
						path.display(),
						needle
					));
				}
			}
		});
	}

	assert!(
		violations.is_empty(),
		"ADR 049 content-hash architectural invariants violated:\n{}",
		violations.join("\n")
	);
}

/// ADR 050 architectural guards.
///
/// PK delete is a supersession behavior on the existing PK version
/// chain. It reuses the `TableDeleteRows` wire frame, the ADR 047
/// tombstone marker column, and ADR 049's admission-time visibility
/// discipline without introducing a new digest domain or reject-code
/// family.
#[test]
fn test_adr050_pk_delete_supersession_boundaries() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let data_server_source =
		fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	for needle in [
		"admit_pk_write_row",
		"admit_pk_delete_row",
		"bifurcate_primary_key_table_write",
		"emit_table_delete_requested_audit",
		"emit_table_delete_completion_audit",
		"tombstone_marker_layout",
		"row_visible_for_admission",
		"pk_delete_outcome_for_latest",
		"TableDeleteRowsRequestMeta",
	] {
		if !data_server_source.contains(needle) {
			violations.push(format!(
				"src/network/data_server.rs: ADR 050 missing required symbol {needle}"
			));
		}
	}

	let pk_delete_source =
		fs::read_to_string(manifest_dir.join("src/storage/pk_delete.rs")).unwrap();
	for needle in [
		"PkAdmissionOutcome",
		"admit_pk_write_row",
		"admit_pk_delete_row",
		"RowIdentityPolicy::PrimaryKey",
		"lookup_as_of",
		"row_visible_under_retention",
	] {
		if !pk_delete_source.contains(needle) {
			violations.push(format!(
				"src/storage/pk_delete.rs: ADR 050 missing required symbol {needle}"
			));
		}
	}

	let metrics_source = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for needle in [
		"wendelon_storage_pk_delete_requests_total",
		"wendelon_storage_pk_delete_rows_deleted_total",
		"wendelon_storage_pk_delete_rows_already_deleted_total",
		"wendelon_storage_pk_delete_rows_not_found_total",
		"wendelon_storage_pk_delete_resurrections_total",
		"wendelon_storage_pk_delete_tombstone_boundary_versions_retained_total",
		"wendelon_storage_pk_upsert_no_digest_add_total",
		"wendelon_storage_pk_admission_visibility_lookup_seconds",
	] {
		if !metrics_source.contains(needle) {
			violations.push(format!(
				"src/metrics/registry.rs: ADR 050 missing required metric {needle}"
			));
		}
	}

	let schema_source = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	if !schema_source.contains("Self::PrimaryKey(_)") {
		violations.push(
			"src/schema/table.rs: PrimaryKey must support row tombstone admission".to_string(),
		);
	}

	let sweeper_source = fs::read_to_string(manifest_dir.join("src/storage/sweeper.rs")).unwrap();
	if !sweeper_source.contains("slab_row_tombstone_metadata(info.slab_id)") {
		violations.push(
			"src/storage/sweeper.rs: fresh tier commits must carry row tombstone metadata"
				.to_string(),
		);
	}

	let manager_source = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	for needle in [
		"row_tombstone_metadata_from_blob(rewritten_plaintext.as_ref()",
		"tombstone_metadata: merge_row_tombstone_metadata(",
	] {
		if !manager_source.contains(needle) {
			violations.push(format!(
				"src/storage/manager.rs: tiered retention rewrites must preserve row tombstone metadata via {needle}"
			));
		}
	}

	let core_source = fs::read_to_string(manifest_dir.join("src/storage/core.rs")).unwrap();
	for needle in [
		"validate_recovered_row_tombstone_marker_shape",
		"row tombstone marker missing __row_table_object_id__",
		"row tombstone marker recovered with user columns",
	] {
		if !core_source.contains(needle) {
			violations.push(format!(
				"src/storage/core.rs: ADR 050 recovery must fail closed for ambiguous tombstone markers via {needle}"
			));
		}
	}

	let scan_roots = ["src", "tests", "benches"];
	let forbidden = [
		concat!("row_currently", "_visible"),
		concat!("PBC_REJECT_ROW_TOMBSTONES_", "REQUIRE_APPEND_ONLY"),
		concat!("PK_DELETE_", "REJECTED_"),
		concat!("PkDelete", "V1"),
		concat!("PkSupersession", "V1"),
		concat!("PkAdmissionOutcome", "V1"),
		concat!("digest.", "table.row.", "p", "k.v1"),
		concat!("digest.", "table.row.", "de", "lete.v1"),
		concat!("digest.", "table.row.", "super", "session.v1"),
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
						"{}: ADR 050 forbidden surface {needle}",
						path.strip_prefix(manifest_dir).unwrap_or(path).display()
					));
				}
			}
		});
	}

	assert!(
		violations.is_empty(),
		"ADR 050 PK-delete supersession invariants violated:\n{}",
		violations.join("\n")
	);
}

/// ADR 051 architectural guards.
///
/// Descriptor fingerprints are split into logical schema identity
/// and physical descriptor shape. Existing aggregate, CSB, columnar,
/// and tiered-stats cache consumers must stay on the logical axis.
#[test]
fn test_adr051_descriptor_fingerprint_split_boundaries() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let fingerprint_source =
		fs::read_to_string(manifest_dir.join("src/hash/descriptor_fingerprint.rs")).unwrap();
	for needle in [
		"pub struct LogicalSchemaFingerprint([u8; 32]);",
		"pub struct PhysicalDescriptorFingerprint([u8; 32]);",
		"pub(crate) const fn from_bytes",
		"pub const fn as_bytes",
		"b\"digest.table.schema.logical.v1\"",
		"b\"digest.table.descriptor.physical.v1\"",
	] {
		if !fingerprint_source.contains(needle) {
			violations.push(format!(
				"src/hash/descriptor_fingerprint.rs: ADR 051 missing required surface {needle}"
			));
		}
	}
	if fingerprint_source.contains("Pod") || fingerprint_source.contains("Zeroable") {
		violations.push(
			"src/hash/descriptor_fingerprint.rs: descriptor fingerprints must not derive Pod/Zeroable"
				.to_string(),
		);
	}

	let schema_source = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	let descriptor_block = schema_source
		.split_once("pub struct TableSchemaDescriptorV1 {")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.unwrap_or("");
	if !descriptor_block.contains("pub logical_schema_fingerprint: LogicalSchemaFingerprint")
		|| !descriptor_block
			.contains("pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint")
	{
		violations.push(
			"src/schema/table.rs: descriptor must carry both ADR 051 fingerprint fields"
				.to_string(),
		);
	}
	for needle in [
		"compute_logical_schema_fingerprint",
		"compute_physical_descriptor_fingerprint",
		"refresh_descriptor_fingerprints",
		"PBC_REJECT_STATS_LOGICAL_FINGERPRINT_MISMATCH",
	] {
		if !schema_source.contains(needle) {
			violations.push(format!(
				"src/schema/table.rs: missing ADR 051 symbol {needle}"
			));
		}
	}

	let aggregate_source =
		fs::read_to_string(manifest_dir.join("src/schema/aggregate.rs")).unwrap();
	if !aggregate_source.contains("pub logical_schema_fingerprint: LogicalSchemaFingerprint") {
		violations.push(
			"src/schema/aggregate.rs: aggregate wire must use LogicalSchemaFingerprint".to_string(),
		);
	}
	if aggregate_source.contains("physical_descriptor_fingerprint") {
		violations.push(
			"src/schema/aggregate.rs: aggregate wire must not bind to physical fingerprint"
				.to_string(),
		);
	}

	let stats_source = fs::read_to_string(manifest_dir.join("src/storage/stats.rs")).unwrap();
	for needle in [
		"pub logical_schema_fingerprint: LogicalSchemaFingerprint",
		"header.logical_schema_fingerprint.as_bytes()",
		"LogicalSchemaFingerprint::from_bytes",
	] {
		if !stats_source.contains(needle) {
			violations.push(format!(
				"src/storage/stats.rs: CSB header must use logical fingerprint via {needle}"
			));
		}
	}
	if stats_source.contains("physical_descriptor_fingerprint") {
		violations.push(
			"src/storage/stats.rs: CSB eligibility must not compare descriptor physical policy"
				.to_string(),
		);
	}

	let columnar_source = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	if !columnar_source.contains("pub logical_schema_fingerprint: LogicalSchemaFingerprint")
		|| !columnar_source.contains("descriptor.logical_schema_fingerprint")
	{
		violations.push(
			"src/storage/columnar.rs: columnar directory must bind logical fingerprint".to_string(),
		);
	}
	if columnar_source.contains("physical_descriptor_fingerprint") {
		violations.push(
			"src/storage/columnar.rs: columnar directory must not bind physical fingerprint"
				.to_string(),
		);
	}

	let manager_source = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	if !manager_source.contains("logical_schema_fingerprint: LogicalSchemaFingerprint")
		|| !manager_source.contains("descriptor.logical_schema_fingerprint")
	{
		violations.push(
			"src/storage/manager.rs: tiered stats cache key must bind logical fingerprint"
				.to_string(),
		);
	}
	if manager_source.contains("physical_descriptor_fingerprint") {
		violations.push(
			"src/storage/manager.rs: tiered stats cache key must not bind physical fingerprint"
				.to_string(),
		);
	}

	let content_hash_source =
		fs::read_to_string(manifest_dir.join("src/storage/content_hash.rs")).unwrap();
	let content_hash_prod = content_hash_source
		.split("\n#[cfg(test)]")
		.next()
		.unwrap_or(content_hash_source.as_str());
	for needle in [
		concat!("logical_schema_", "fingerprint"),
		concat!("physical_descriptor_", "fingerprint"),
		concat!("schema_", "fingerprint"),
	] {
		if content_hash_prod.contains(needle) {
			violations.push(format!(
				"src/storage/content_hash.rs: content-hash row identity production path must stay descriptor-fingerprint-free ({needle})"
			));
		}
	}

	let pk_delete_source =
		fs::read_to_string(manifest_dir.join("src/storage/pk_delete.rs")).unwrap();
	for needle in [
		concat!("logical_schema_", "fingerprint"),
		concat!("physical_descriptor_", "fingerprint"),
		concat!("schema_", "fingerprint"),
	] {
		if pk_delete_source.contains(needle) {
			violations.push(format!(
				"src/storage/pk_delete.rs: PK delete admission must stay descriptor-fingerprint-free ({needle})"
			));
		}
	}

	let scan_roots = ["src", "tests", "benches"];
	let forbidden = [
		concat!("schema", "_fingerprint"),
		concat!("Schema", "Fingerprint"),
		concat!("PBC_REJECT_STATS_", "FINGERPRINT_MISMATCH"),
		concat!("PBC_REJECT_STATS_PHYSICAL_", "FINGERPRINT_MISMATCH"),
		concat!("LogicalSchemaFingerprint", "V1"),
		concat!("PhysicalDescriptorFingerprint", "V1"),
		concat!("DescriptorFingerprint", "V1"),
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
				if content
					.split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
					.any(|token| token == needle)
				{
					violations.push(format!(
						"{}: ADR 051 forbidden descriptor fingerprint token {needle}",
						path.strip_prefix(manifest_dir).unwrap_or(path).display()
					));
				}
			}
		});
	}

	let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	if !cargo_toml.contains("name = \"descriptor_fingerprint\"") {
		violations
			.push("Cargo.toml: ADR 051 descriptor fingerprint bench is not registered".to_string());
	}

	let metrics_source = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for needle in [
		"wendelon_storage_csb_merge_logical_fingerprint_mismatch_total",
		"wendelon_storage_csb_payload_coverage_fall_through_total",
		"wendelon_storage_csb_merge_pre_adr_051_cliff_total",
		"wendelon_storage_aggregate_cache_logical_fingerprint_hit_total",
		"wendelon_storage_aggregate_cache_logical_fingerprint_miss_total",
		"wendelon_storage_descriptor_recompute_seconds",
		"wendelon_storage_descriptor_validate_seconds",
		"wendelon_storage_descriptor_logical_only_mutations_total",
		"wendelon_storage_descriptor_physical_only_mutations_total",
		"wendelon_storage_descriptor_combined_mutations_total",
	] {
		if !metrics_source.contains(needle) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 051 metric {needle}"
			));
		}
	}

	if !manifest_dir
		.join("plans/ops/runbooks/descriptor_fingerprint_split.md")
		.exists()
	{
		violations.push(
			"plans/ops/runbooks/descriptor_fingerprint_split.md: ADR 051 runbook is missing"
				.to_string(),
		);
	}
	let runbook_index =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/README.md")).unwrap();
	if !runbook_index.contains("descriptor_fingerprint_split.md") {
		violations.push("plans/ops/runbooks/README.md: ADR 051 runbook is not listed".to_string());
	}
	let runbook =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/descriptor_fingerprint_split.md"))
			.unwrap();
	if !runbook.contains("table.schema_descriptor_updated") {
		violations.push(
			"plans/ops/runbooks/descriptor_fingerprint_split.md: ADR 051 audit event is not documented"
				.to_string(),
		);
	}
	let audit_source = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	if !audit_source.contains("table.schema_descriptor_updated")
		|| !audit_source.contains("TableSchemaDescriptorUpdate")
	{
		violations
			.push("src/audit.rs: ADR 051 descriptor mutation audit surface is missing".to_string());
	}

	assert!(
		violations.is_empty(),
		"ADR 051 descriptor fingerprint invariants violated:\n{}",
		violations.join("\n")
	);
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
