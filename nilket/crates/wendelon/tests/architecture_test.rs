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
/// - The `__row_tombstone__` synthetic column is deferred to a
///   follow-up ADR and must not appear in ADR 045 scope.
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
			// Tombstone leak guard: ADR 045 must not extend the
			// `__row_tombstone__` synthetic-column surface. The
			// pre-existing `StorageColumn::RowTombstone` slot in
			// `src/storage/columnar.rs` predates ADR 045 and is the
			// only authorized reference.
			let is_columnar_predecessor = path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name == "columnar.rs")
				&& path
					.parent()
					.and_then(|parent| parent.file_name())
					.and_then(|name| name.to_str())
					.is_some_and(|name| name == "storage");
			if !is_columnar_predecessor && content.contains("__row_tombstone__") {
				violations.push(format!(
					"{}: __row_tombstone__ is deferred to a follow-up ADR; must not appear in \
					 ADR 045 scope",
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

		if content.contains("__row_tombstone__") {
			let is_columnar_predecessor = relative == Path::new("src/storage/columnar.rs");
			if !is_columnar_predecessor {
				violations.push(format!(
					"{}: ADR 046 must not introduce the deferred __row_tombstone__ column",
					relative.display()
				));
			}
		}
		if content.contains("AppendOnlyContentHash") {
			violations.push(format!(
				"{}: content-hash identity is deferred and must not be pre-allocated",
				relative.display()
			));
		}
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
