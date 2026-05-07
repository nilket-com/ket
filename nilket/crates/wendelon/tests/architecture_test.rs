use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use proc_macro2::{TokenStream, TokenTree};

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

fn rust_identifiers_from_source(source_name: &str, source: &str) -> Vec<String> {
	let tokens = TokenStream::from_str(source).unwrap_or_else(|err| {
		panic!("{source_name}: Rust tokenization failed: {err}");
	});
	let mut identifiers = Vec::new();
	collect_identifiers(tokens, &mut identifiers);
	identifiers
}

fn collect_identifiers(tokens: TokenStream, identifiers: &mut Vec<String>) {
	for token in tokens {
		match token {
			TokenTree::Ident(ident) => identifiers.push(ident.to_string()),
			TokenTree::Group(group) => collect_identifiers(group.stream(), identifiers),
			TokenTree::Punct(_) | TokenTree::Literal(_) => {}
		}
	}
}

fn has_rust_identifier(identifiers: &[String], ident: &str) -> bool {
	identifiers.iter().any(|candidate| candidate == ident)
}

#[test]
fn test_adr055_lsm_compaction_substrate_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let storage_lsm = fs::read_to_string(manifest_dir.join("src/storage/lsm.rs")).unwrap();
	let storage_manifest =
		fs::read_to_string(manifest_dir.join("src/storage/manifest.rs")).unwrap();
	let storage_worker =
		fs::read_to_string(manifest_dir.join("src/storage/lsm_worker.rs")).unwrap();
	let storage_manager = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let storage_index = fs::read_to_string(manifest_dir.join("src/storage/index.rs")).unwrap();
	let slab_pins = fs::read_to_string(manifest_dir.join("src/storage/slab_pins.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let consensus_core = fs::read_to_string(manifest_dir.join("src/consensus/core.rs")).unwrap();
	let raft_storage =
		fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let storage_sweeper = fs::read_to_string(manifest_dir.join("src/storage/sweeper.rs")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let transport = fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let lsm_runbook =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/lsm_compaction.md")).unwrap();

	let mut violations = Vec::new();

	if units.matches("pub struct CompactionLevelIndex").count() != 1 {
		violations.push(
			"src/schema/units.rs: expected exactly one CompactionLevelIndex definition".to_string(),
		);
	}
	if units.contains("pub struct LevelIndex") {
		violations.push(
			"src/schema/units.rs: bare LevelIndex is ambiguous with L1/L2 storage tiers"
				.to_string(),
		);
	}
	for required in [
		"pub const MAX_RAW: u8 = 3;",
		"pub const L0: Self = Self(0);",
		"pub const L3: Self = Self(3);",
		"Self::try_new(raw)",
		"invalid LSM compaction level",
	] {
		if !units.contains(required) {
			violations.push(format!(
				"src/schema/units.rs: missing CompactionLevelIndex range guard token {required}"
			));
		}
	}
	for required in [
		"impl_unit!(CompactionJobId, u64);",
		"impl_unit!(ManifestEpoch, u64);",
	] {
		if !units.contains(required) {
			violations.push(format!(
				"src/schema/units.rs: missing ADR 055 unit {required}"
			));
		}
	}

	if storage_lsm.matches("pub struct CompactionMergeKey").count() != 1 {
		violations.push(
			"src/storage/lsm.rs: expected exactly one CompactionMergeKey definition".to_string(),
		);
	}
	for cdc_path in [
		"src/schema/cdc.rs",
		"src/storage/cdc.rs",
		"src/network/cdc.rs",
	] {
		let content = fs::read_to_string(manifest_dir.join(cdc_path)).unwrap_or_default();
		if content.contains("CompactionMergeKey") || content.contains("CompactionLevelIndex") {
			violations.push(format!(
				"{cdc_path}: CDC must not import LSM compaction key/level types"
			));
		}
	}

	for required in [
		"pub enum SlabState",
		"Retired { successors: Vec<SlabId> }",
		"pub struct LsmRunSummary",
		"pub compaction_level: CompactionLevelIndex",
		"pub enum LsmRejectCode",
		"ManifestRace = 1",
		"SidecarDigestMismatch = 2",
		"VisibleDigestInvariance = 3",
		"pub enum VersionLifecycle",
		"pub struct LsmSourceFrame",
		"pub struct VisibleDomainRow",
		"compaction_visible_domain_summary",
		"pub fn apply_elision",
		"pub struct PlannedOutputRun",
		"pub fn plan_output_runs",
		"pub struct LsmConfig",
		"lsm_compaction_concurrency_per_locality: 1",
		"output_run_target_bytes",
		"pub fn pick_compaction",
		"pub fn touched_slabs",
	] {
		if !storage_lsm.contains(required) {
			violations.push(format!(
				"src/storage/lsm.rs: missing ADR 055 token {required}"
			));
		}
	}
	for required in [
		"pub struct ManifestSnapshot",
		"slabs_at_compaction_level",
		"overlapping_slabs",
		"oldest_l0_slab_age_millis",
		"compaction_level_total_bytes",
		"compaction_level_slab_count",
		"apply_compaction_commit",
		"validate_compaction_commit_against_plan",
		"apply_planned_compaction_commit",
	] {
		if !storage_manifest.contains(required) {
			violations.push(format!(
				"src/storage/manifest.rs: missing ADR 055 token {required}"
			));
		}
	}
	for required in [
		"pub struct SlabPinTable",
		"pub fn acquire(&self, slabs: &[SlabId])",
		"pub struct SlabPinGuard",
		"impl Drop for SlabPinGuard",
		"SlabAlreadyDeletePending",
	] {
		if !slab_pins.contains(required) {
			violations.push(format!(
				"src/storage/slab_pins.rs: missing ADR 055 token {required}"
			));
		}
	}
	for required in [
		"compaction_manifest_snapshot",
		"reserve_lsm_output_slab_ids",
		"lsm_version_entries_for_plan",
		"prepare_lsm_compaction_outputs",
		"read_lsm_single_row_source_frames",
		"apply_lsm_manifest_compaction_commit",
		"apply_lsm_manifest_planned_compaction_commit",
		"install_lsm_compaction_temp_outputs",
		"install_lsm_compaction_temp_outputs_via_control_ring",
		"run_lsm_compaction_once",
		"pick_lsm_compaction_plan",
		"run_lsm_compaction_tick",
		"run_lsm_compaction_tick_via_control_ring",
		"run_lsm_compaction_tick_via_control_ring_with_catalogs",
		"reconcile_lsm_secondary_index_segments_after_compaction",
		"sweep_lsm_delete_pending_once",
		"sweep_lsm_delete_pending_once_via_control_ring",
		"cleanup_lsm_compaction_tmp_files",
		"lsm_source_frame_table_object_id",
		"compaction-temp-",
		"slab_pin_table",
		"pin_lsm_read_location",
	] {
		if !storage_manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 055 manager token {required}"
			));
		}
	}
	for required in [
		"LSM_RUNS_TABLE",
		"lsm_runs_v1",
		"pub lsm_runs: Vec<LsmRunSummary>",
		"pub put_lsm_runs: Vec<LsmRunSummary>",
		"pub delete_lsm_run_ids: Vec<SlabId>",
		"encode_lsm_run_summary",
		"decode_lsm_run_summary",
	] {
		if !storage_index.contains(required) {
			violations.push(format!(
				"src/storage/index.rs: missing ADR 055 persisted run token {required}"
			));
		}
	}
	for required in [
		"pub struct LsmCompactionWorker",
		"TableLifecycleCatalog",
		"retention_floor_from_catalog",
		"acquire_plan_pins",
		"pub struct LsmPreparedOutputs",
		"plan_survivor_outputs",
		"pub trait LsmCompactionDriver",
		"pub trait LsmCompactionJobRunner",
		"pub struct LsmCompactionService",
		"pub struct LsmCompactionServiceControl",
		"pub fn spawn(self) -> JoinHandle<()>",
		"pub async fn run_one",
		"build_compaction_commit",
		"compaction_outcome_from_commit",
		"pub struct LsmGcWorker",
		"pub trait LsmGcDriver",
		"pub struct LsmGcService",
		"pub struct LsmGcPlan",
		"transition_retired_to_delete_pending",
		"transition_retired_to_delete_pending_if",
	] {
		if !storage_worker.contains(required) {
			violations.push(format!(
				"src/storage/lsm_worker.rs: missing ADR 055 token {required}"
			));
		}
	}

	for required in [
		"wendelon_lsm_compactions_total",
		"wendelon_lsm_compaction_input_slabs",
		"wendelon_lsm_compaction_output_bytes",
		"wendelon_lsm_compaction_duration_millis",
		"wendelon_lsm_level_slab_count",
		"wendelon_lsm_level_total_bytes",
		"wendelon_lsm_pending_delete_slabs",
		"wendelon_lsm_retired_slab_pinned_bytes",
		"wendelon_lsm_digest_invariance_violations_total",
		"result.as_str()",
		"kind.as_str()",
	] {
		if !metrics.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 055 metric token {required}"
			));
		}
	}
	for forbidden in ["tenant_id", "table_object_id", "subscription_id", "slab_id"] {
		for line in metrics
			.lines()
			.filter(|line| line.contains("wendelon_lsm_"))
		{
			if line.contains(forbidden) {
				violations.push(format!(
					"src/metrics/registry.rs: LSM metric line carries forbidden high-cardinality label {forbidden}: {line}"
				));
			}
		}
	}

	for forbidden in [
		"MsgType::LSM",
		"MsgType::COMPACTION",
		"FRAME_FLAG_LSM",
		"FRAME_FLAG_COMPACTION",
	] {
		if schema_core.contains(forbidden) {
			violations.push(format!(
				"src/schema/core.rs: forbidden ADR 055 wire token {forbidden}"
			));
		}
	}
	for forbidden in [
		"NodeCapability::Lsm",
		"NodeCapability::Compaction",
		"NodeCapability::LSM",
		"NodeCapability::COMPACTION",
	] {
		if transport.contains(forbidden) {
			violations.push(format!(
				"src/network/transport.rs: forbidden ADR 055 capability token {forbidden}"
			));
		}
	}

	for source in [&storage_lsm, &storage_manifest, &storage_worker, &slab_pins] {
		if source.contains("StorageTier") {
			violations.push("LSM substrate must not embed StorageTier".to_string());
		}
		if source.contains("DigestEvictionMarker") {
			violations.push("LSM substrate must not add DigestEvictionMarker".to_string());
		}
		if source.contains("relocated_reads") {
			violations.push("LSM substrate must not write or embed relocated_reads".to_string());
		}
	}
	for source in [&consensus_core, &raft_storage] {
		if source.contains("DigestEvictionMarker") {
			violations.push("LSM control-ring path must not add DigestEvictionMarker".to_string());
		}
	}

	for forbidden in [
		"LsmV1",
		"LsmV2",
		"CompactionV1",
		"CompactionV2",
		"WendelonLsm",
		"WendelonCompaction",
	] {
		for source in [
			&units,
			&storage_lsm,
			&storage_manifest,
			&storage_worker,
			&slab_pins,
		] {
			if source.contains(forbidden) {
				violations.push(format!("ADR 055 forbidden suffix/prefix token {forbidden}"));
			}
		}
	}

	for required in [
		"LsmCompactionCommitted",
		"LsmInvariantViolation",
		"LsmCompactionPaused",
		"lsm.compaction_committed",
		"lsm.invariant_violation",
		"lsm.compaction_paused",
	] {
		if !audit.contains(required) {
			violations.push(format!(
				"src/audit.rs: missing ADR 055 audit token {required}"
			));
		}
	}

	for required in ["CompactionCommit(CompactionCommit)", "control ring"] {
		if !consensus_core.contains(required) {
			violations.push(format!(
				"src/consensus/core.rs: missing ADR 055 control-ring token {required}"
			));
		}
	}
	for required in [
		"WendelonRequest::CompactionCommit",
		"TierObserverEvent::LsmCompactionCommitted",
		"on_lsm_compaction_committed",
		"AuditOperation::LsmCompactionCommitted",
		"AuditOperation::LsmInvariantViolation",
	] {
		if !raft_storage.contains(required) {
			violations.push(format!(
				"src/consensus/raft_storage.rs: missing ADR 055 control-ring apply token {required}"
			));
		}
	}
	for required in [
		"on_lsm_compaction_committed",
		"apply_lsm_manifest_compaction_commit(commit)",
		"commit_lsm_compaction",
		"WendelonRequest::CompactionCommit(commit)",
	] {
		if !storage_sweeper.contains(required) {
			violations.push(format!(
				"src/storage/sweeper.rs: missing ADR 055 manager observer token {required}"
			));
		}
	}
	for required in [
		"LeaderGatedLsmCompactionDriver",
		"impl LsmCompactionDriver for LeaderGatedLsmCompactionDriver",
		"if !self.leader.is_leader().await",
		"committer: Arc<dyn TierCommitter>",
		"state_machine: WendelonStateMachine",
		"run_lsm_compaction_tick_via_control_ring",
		"run_lsm_compaction_tick_via_control_ring_with_catalogs",
		"lsm_compaction_paused",
		"lsm_compaction_control",
		"LsmCompactionService::new",
		"LeaderGatedLsmGcDriver",
		"sweep_lsm_delete_pending_once_via_control_ring",
		"LsmGcService::new",
		"lsm_gc",
		"lsm_compaction",
	] {
		if !node.contains(required) {
			violations.push(format!(
				"src/node.rs: missing ADR 055 node service token {required}"
			));
		}
	}

	for required in ["[[bench]]", "name = \"lsm_compaction\"", "harness = false"] {
		if !cargo_toml.contains(required) {
			violations.push(format!(
				"Cargo.toml: missing ADR 055 bench token {required}"
			));
		}
	}
	for required in [
		"wendelon_lsm_compactions_total{level,result}",
		"wendelon_lsm_retired_slab_pinned_bytes",
		"sidecar_digest_mismatch",
		"visible_digest_invariance",
		"cargo bench --bench lsm_compaction -- --quick",
	] {
		if !lsm_runbook.contains(required) {
			violations.push(format!(
				"plans/ops/runbooks/lsm_compaction.md: missing ADR 055 runbook token {required}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 055 architecture guard violations:\n{}",
		violations.join("\n")
	);
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
			let relative = path.strip_prefix(manifest_dir).unwrap_or(path);
			if content.contains("curve25519_dalek") {
				violations.push(format!(
					"{}: curve25519-dalek must stay out of network/storage hot paths",
					path.display()
				));
			}
			let state_digest_storage_surface = relative
				== Path::new("src/storage/secondary_index_segment.rs")
				|| relative == Path::new("src/storage/lsm.rs");
			if content.contains("StateDigest") && !state_digest_storage_surface {
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

#[test]
fn test_adr052_secondary_index_architecture_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let state_digest = fs::read_to_string(manifest_dir.join("src/hash/state_digest.rs")).unwrap();
	if !state_digest.contains("DOMAIN_SECONDARY_INDEX_ENTRY")
		|| !state_digest.contains("digest.secondary_index.entry.v1")
	{
		violations.push(
			"src/hash/state_digest.rs: ADR 052 secondary-index entry digest domain is missing"
				.to_string(),
		);
	}

	let schema_source =
		fs::read_to_string(manifest_dir.join("src/schema/secondary_index.rs")).unwrap();
	for required in [
		"INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH",
		"INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE",
		"INDEX_REJECT_PK_COLUMN",
		"INDEX_REJECT_SYNTHETIC_COLUMN",
		"SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED",
		"SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION",
		"SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION",
		"SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION",
	] {
		if !schema_source.contains(required) {
			violations.push(format!(
				"src/schema/secondary_index.rs: missing ADR 052 reject code {required}"
			));
		}
	}

	let scan_roots = ["src", "tests", "benches"];
	for root in scan_roots {
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
			for forbidden in [
				concat!("INDEX_REJECT_", "ALTER"),
				concat!("INDEX_REJECT_", "NULL"),
				concat!("SecondaryIndexDescriptor", "V1"),
				concat!("SecondaryIndexEntry", "V1"),
				concat!("SecondaryIndexCatalog", "V1"),
				concat!("IndexLogical", "Fingerprint"),
				concat!("digest.table.index.descriptor", ".logical.v1"),
				concat!("Wendelon", "Index"),
				concat!("wendelon", "_index_"),
			] {
				if content.contains(forbidden) {
					violations.push(format!(
						"{}: contains forbidden ADR 052 token '{}'",
						path.display(),
						forbidden
					));
				}
			}
		});
	}

	let secondary_index_sources = [
		"src/schema/secondary_index.rs",
		"src/storage/secondary_index.rs",
		"src/storage/secondary_index_segment.rs",
	];
	for relative in secondary_index_sources {
		let content = fs::read_to_string(manifest_dir.join(relative)).unwrap();
		if content.contains("PhysicalDescriptorFingerprint") {
			violations.push(format!(
				"{relative}: ADR 052 index entries must not bind to the physical descriptor fingerprint"
			));
		}
	}

	let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	if !manifest.contains("name = \"secondary_index\"") {
		violations.push("Cargo.toml: ADR 052 secondary_index benchmark is missing".to_string());
	}
	let bench_source = fs::read_to_string(manifest_dir.join("benches/secondary_index.rs")).unwrap();
	if !bench_source.contains("--full") || !bench_source.contains("1_000_000") {
		violations.push(
			"benches/secondary_index.rs: ADR 052 promotion-gate row-count mode is missing"
				.to_string(),
		);
	}

	let runbook_path = manifest_dir.join("plans/ops/runbooks/secondary_indexes.md");
	if !runbook_path.exists() {
		violations.push(
			"plans/ops/runbooks/secondary_indexes.md: ADR 052 operator runbook is missing"
				.to_string(),
		);
	} else {
		let runbook = fs::read_to_string(&runbook_path).unwrap();
		for required in [
			"wendelon_storage_secondary_index_uncovered_slabs_total",
			"wendelon_storage_secondary_index_corruption_failures_total",
			"secondary_index.catalog_updated",
			"IndexDropGcComplete",
			"SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION",
			"max_index_buffer_bytes",
			"active_buffer_overflow",
		] {
			if !runbook.contains(required) {
				violations.push(format!(
					"plans/ops/runbooks/secondary_indexes.md: missing ADR 052 runbook token {required}"
				));
			}
		}
	}

	let runbook_catalog =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/README.md")).unwrap();
	if !runbook_catalog.contains("secondary_indexes.md") {
		violations
			.push("plans/ops/runbooks/README.md: ADR 052 runbook is not cataloged".to_string());
	}

	let metrics_source = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for required in [
		"wendelon_storage_secondary_index_segments_built_total",
		"wendelon_storage_secondary_index_segments_failed_total",
		"wendelon_storage_secondary_index_query_plans_total",
		"wendelon_storage_secondary_index_drop_gc_seconds",
	] {
		if !metrics_source.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 052 metric {required}"
			));
		}
	}

	let manager_source = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	for required in [
		"gc_secondary_index_segments_for_dropping_index",
		"CorruptSegment",
		"forget_slab",
		"observe_storage_secondary_index_corruption_failure",
		"source_slab_lease",
		"acquire_secondary_index_source_slab_read_lease",
	] {
		if !manager_source.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 052 manager surface {required}"
			));
		}
	}

	let audit_source = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	for required in [
		"SecondaryIndexCatalogUpdated",
		"secondary_index.catalog_updated",
		"SecondaryIndexCatalogUpdate",
		"target_index_object_id",
	] {
		if !audit_source.contains(required) {
			violations.push(format!(
				"src/audit.rs: missing ADR 052 audit surface {required}"
			));
		}
	}

	let metrics_smoke = fs::read_to_string(manifest_dir.join("tests/metrics_smoke.rs")).unwrap();
	if !metrics_smoke.contains("wendelon_storage_secondary_index_drop_gc_seconds") {
		violations
			.push("tests/metrics_smoke.rs: ADR 052 metrics smoke coverage is missing".to_string());
	}

	assert!(
		violations.is_empty(),
		"ADR 052 secondary-index invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr053_wire_ipc_compression_architecture_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let network_ipc = fs::read_to_string(manifest_dir.join("src/network/ipc.rs")).unwrap();
	for required in [
		"WIRE_BODY_COMPRESSION",
		"RecordBodyCompressionPolicy::Lz4Frame",
		"wire_ipc_write_options",
		"wire_encode_batch",
		"wire_decode_batch",
		"wire_encode_batch_for_path",
		"observe_wire_decode_error_for_bytes",
		"descriptor-schema: `src/schema/table.rs::encode_arrow_schema_ipc`",
		"test-or-bench-default",
	] {
		if !network_ipc.contains(required) {
			violations.push(format!(
				"src/network/ipc.rs: missing ADR 053 wire IPC surface {required}"
			));
		}
	}

	let network_mod = fs::read_to_string(manifest_dir.join("src/network/mod.rs")).unwrap();
	if !network_mod.contains("pub mod ipc;") || !network_mod.contains("pub use ipc::*;") {
		violations
			.push("src/network/mod.rs: ADR 053 network IPC module is not exported".to_string());
	}

	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	if !schema_core.contains("FRAME_FLAG_KNOWN_MASK")
		|| !schema_core.contains("pub const FRAME_FLAG_KNOWN_MASK: u16 = FRAME_FLAG_STREAM_END;")
	{
		violations
			.push("src/schema/core.rs: ADR 053 known HRFT frame flag mask is missing".to_string());
	}
	let network_core = fs::read_to_string(manifest_dir.join("src/network/core.rs")).unwrap();
	if !network_core.contains("header.flags & !FRAME_FLAG_KNOWN_MASK") {
		violations.push(
			"src/network/core.rs: ADR 053 unknown frame flag rejection is missing".to_string(),
		);
	}
	if !schema_core.contains("pub const HRFT_ENVELOPE_VERSION: u8 = 1;") {
		violations
			.push("src/schema/core.rs: ADR 053 must not bump HRFT_ENVELOPE_VERSION".to_string());
	}
	for forbidden in [concat!("arrow_", "compressed"), concat!("has_", "trailing")] {
		if schema_core.contains(forbidden) {
			violations.push(format!(
				"src/schema/core.rs: FrameHeader.flags doc still contains forbidden ADR 053 token {forbidden}"
			));
		}
	}

	let client_core = fs::read_to_string(manifest_dir.join("src/client/core.rs")).unwrap();
	if !client_core.contains("NetworkCompressionPath::ClientBatch")
		|| !client_core.contains("StreamWriter::try_new_with_options")
		|| client_core.contains("StreamWriter::try_new(")
	{
		violations.push(
			"src/client/core.rs: client batch wire encoder must use compressed IPC options"
				.to_string(),
		);
	}

	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let production_data_server = data_server
		.split("\n#[cfg(test)]\nmod tests")
		.next()
		.unwrap_or(data_server.as_str());
	if production_data_server.contains("StreamWriter::try_new(") {
		violations.push(
			"src/network/data_server.rs: migration writer must not use bare StreamWriter::try_new"
				.to_string(),
		);
	}
	for (path, minimum) in [
		("NetworkCompressionPath::MigrationRecords", 1),
		("NetworkCompressionPath::QueryResponse", 3),
		("NetworkCompressionPath::ReadResponse", 1),
	] {
		let count = production_data_server.matches(path).count();
		if count < minimum {
			violations.push(format!(
				"src/network/data_server.rs: ADR 053 expected at least {minimum} {path} encoder site(s), found {count}"
			));
		}
	}
	for forbidden in [
		"let arrow_bytes = encode_batch(",
		"let arrow = encode_batch(",
		"let bytes = encode_batch(",
	] {
		if production_data_server.contains(forbidden) {
			violations.push(format!(
				"src/network/data_server.rs: production wire response still calls storage multirow encode_batch via `{forbidden}`"
			));
		}
	}
	for (relative, content) in [
		("src/client/core.rs", client_core.as_str()),
		("src/network/data_server.rs", production_data_server),
	] {
		if content.contains("StreamWriter::try_new_with_options")
			&& !content.contains("wire_ipc_write_options")
		{
			violations.push(format!(
				"{relative}: wire-side StreamWriter::try_new_with_options must route through wire_ipc_write_options"
			));
		}
	}
	let storage_ipc = fs::read_to_string(manifest_dir.join("src/storage/ipc.rs")).unwrap();
	if !storage_ipc.contains("ipc_write_options_for_policy") {
		violations
			.push("src/storage/ipc.rs: ADR 043 storage IPC policy helper is missing".to_string());
	}

	let metrics_source = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for required in [
		"network_compression_batches_total",
		"network_compression_encoded_bytes_total",
		"network_compression_encode_micros",
		"network_compression_ratio",
		"network_compression_decode_error_total",
		"client_batch",
		"migration_records",
		"query_response",
		"read_response",
		"compressed",
		"error",
	] {
		if !metrics_source.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 053 network compression metric token {required}"
			));
		}
	}
	let runbook_path = manifest_dir.join("plans/ops/runbooks/wire_arrow_ipc_body_compression.md");
	if !runbook_path.exists() {
		violations
			.push("plans/ops/runbooks/wire_arrow_ipc_body_compression.md is missing".to_string());
	} else {
		let runbook = fs::read_to_string(&runbook_path).unwrap();
		for required in [
			"wendelon_network_compression_batches_total",
			"wendelon_network_compression_decode_error_total",
			"storage_compression_*",
			"FRAME_FLAG_STREAM_END",
			"Identity and locality",
		] {
			if !runbook.contains(required) {
				violations.push(format!(
					"plans/ops/runbooks/wire_arrow_ipc_body_compression.md: missing ADR 053 runbook token {required}"
				));
			}
		}
	}
	let runbook_catalog =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/README.md")).unwrap();
	if !runbook_catalog.contains("wire_arrow_ipc_body_compression.md") {
		violations
			.push("plans/ops/runbooks/README.md: ADR 053 runbook is not cataloged".to_string());
	}
	let observability =
		fs::read_to_string(manifest_dir.join("plans/ops/observability.md")).unwrap();
	for required in [
		"wendelon_network_compression_batches_total",
		"wendelon_network_compression_encoded_bytes_total",
		"wendelon_network_compression_encode_micros",
		"wendelon_network_compression_ratio",
		"wendelon_network_compression_decode_error_total",
	] {
		if !observability.contains(required) {
			violations.push(format!(
				"plans/ops/observability.md: missing ADR 053 metric {required}"
			));
		}
	}

	for root in ["src", "tests", "benches"] {
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
			for forbidden in [
				concat!("network_compression_plain", "text_bytes_total"),
				concat!("network_compression_decode", "_micros"),
				concat!("uncompressed_", "passthrough"),
				concat!("network_compression_decode_error_total{codec, ", "reason"),
				concat!("MsgType::", "ARROW_COMPRESSED_"),
				concat!("FRAME_FLAG_", "COMPRESS"),
				concat!("Wire", "Body", "Compression", "Policy"),
				concat!("Wire", "Compression", "Level"),
				concat!("Record", "Body", "Compression", "Policy", "V"),
				concat!("Wire", "Compression", "V"),
				concat!("Wendelon", "WireCompression"),
				concat!("wendelon", "_wire_compression_"),
			] {
				if content.contains(forbidden) {
					violations.push(format!(
						"{}: contains forbidden ADR 053 token '{}'",
						path.display(),
						forbidden
					));
				}
			}
			for line in content.lines() {
				if line.contains("StreamReader::try_new(") && !line.contains(", None)") {
					violations.push(format!(
						"{}: ADR 053 receiver StreamReader must pass projection None: {}",
						path.display(),
						line.trim()
					));
				}
			}
		});
	}

	for manifest in ["Cargo.toml", "crates/wendelon-client/Cargo.toml"] {
		let content = fs::read_to_string(manifest_dir.join(manifest)).unwrap();
		if !content.contains("arrow = {") || !content.contains("\"ipc_compression\"") {
			violations.push(format!(
				"{manifest}: ADR 053 requires Arrow ipc_compression feature to stay pinned on"
			));
		}
	}
	let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	if !cargo_toml.contains("name = \"wire_arrow_ipc_body_compression\"") {
		violations.push("Cargo.toml: ADR 053 wire compression benchmark is missing".to_string());
	}
	let bench_path = manifest_dir.join("benches/wire_arrow_ipc_body_compression.rs");
	if !bench_path.exists() {
		violations.push(
			"benches/wire_arrow_ipc_body_compression.rs: ADR 053 bench harness is missing"
				.to_string(),
		);
	} else {
		let bench = fs::read_to_string(&bench_path).unwrap();
		for required in [
			"--quick",
			"--full",
			"encode_wire_lz4",
			"decode_wire_lz4",
			"wire_arrow_ipc_size",
			"wire_arrow_ipc_rpc_size",
			"wire_arrow_ipc_transmit_estimate",
			"rpc_{rpc}_default_encode_decode",
			"rpc_{rpc}_wire_lz4_encode_decode",
			"rpc_read_resp_pass_through_decode_only",
			"RecordBodyCompressionPolicy::Lz4Frame",
		] {
			if !bench.contains(required) {
				violations.push(format!(
					"benches/wire_arrow_ipc_body_compression.rs: missing ADR 053 bench token {required}"
				));
			}
		}
	}
	let bench_results =
		manifest_dir.join("plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md");
	if !bench_results.exists() {
		violations.push(
			"plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md is missing"
				.to_string(),
		);
	} else {
		let bench_results = fs::read_to_string(&bench_results).unwrap();
		for required in [
			"2026-05-04 Quick Smoke",
			"wire_helper_encode_in_timed_path=false",
			"full promotion matrix",
		] {
			if !bench_results.contains(required) {
				violations.push(format!(
					"plans/impl/053_wire_arrow_ipc_body_compression_bench_results.md: missing ADR 053 bench-results token {required}"
				));
			}
		}
	}
	let transport_source =
		fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	if !transport_source.contains("pub const ALL: [NodeCapability; 2]") {
		violations.push(
			"src/network/transport.rs: ADR 053 must not add a node capability bit".to_string(),
		);
	}

	assert!(
		violations.is_empty(),
		"ADR 053 wire IPC compression invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn adr054_cdc_subscription_substrate_contract() {
	let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let schema_cdc = fs::read_to_string(manifest_dir.join("src/schema/cdc.rs")).unwrap();
	for required in [
		"pub struct CdcCursor",
		"pub row_hlc: Hlc",
		"pub row_key_id: RowKeyId",
		"pub record_row_index: RecordRowIndex",
		"pub struct CdcSubscriptionId(pub u128)",
		"pub enum CdcSubscribeBound",
		"pub enum CdcFrameSource",
		"pub enum CdcRejectCode",
		"BelowRetentionFloor = 1",
		"InternalEngineError = 8",
		"pub struct CdcSubscribeRequestMeta",
		"resume_visible_state_digest: StateDigest",
		"pub struct CdcFrameMeta",
		"frame_low_cursor",
		"frame_high_cursor",
		"pub struct CdcCheckpointMeta",
		"digest_domain_raw: u32",
		"pub struct CdcRejectMeta",
		"code_raw: u32",
		"pub fn cdc_event_batch",
		"pub fn cdc_tombstone_event_batch",
		"pub fn cdc_cursor_in_catchup_window",
		"cursor > start_exclusive && cursor <= upper_inclusive",
	] {
		if !schema_cdc.contains(required) {
			violations.push(format!(
				"src/schema/cdc.rs: missing ADR 054 token {required}"
			));
		}
	}
	for forbidden in [
		"CdcRejectCode, Pod",
		"CdcSubscribeBound, Pod",
		"CdcFrameSource, Pod",
		concat!("Cdc", "EventKind"),
		concat!("Cdc", "Operation"),
	] {
		if schema_cdc.contains(forbidden) {
			violations.push(format!(
				"src/schema/cdc.rs: contains forbidden ADR 054 token {forbidden}"
			));
		}
	}

	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	for required in [
		"pub const CDC_SUBSCRIBE_REQ: Self = Self(0x36)",
		"pub const CDC_FRAME: Self = Self(0x37)",
		"pub const CDC_CHECKPOINT: Self = Self(0x38)",
		"pub const CDC_REJECT: Self = Self(0x39)",
	] {
		if !schema_core.contains(required) {
			violations.push(format!(
				"src/schema/core.rs: missing ADR 054 MsgType {required}"
			));
		}
	}
	if schema_core.contains(concat!("FRAME_FLAG_", "CDC")) {
		violations.push("src/schema/core.rs: ADR 054 must not add CDC frame flags".to_string());
	}

	let network_cdc = fs::read_to_string(manifest_dir.join("src/network/cdc.rs")).unwrap();
	for required in [
		"CdcSubscriptionRegistry",
		"SubscriptionHandle",
		"cdc_emit_frame",
		"wire_encode_batch_for_path(batch, NetworkCompressionPath::CdcEvent)",
		"cdc_emit_checkpoint",
		"cdc_emit_reject",
		"pub fn remove",
	] {
		if !network_cdc.contains(required) {
			violations.push(format!(
				"src/network/cdc.rs: missing ADR 054 token {required}"
			));
		}
	}

	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	for required in [
		"MsgType::CDC_SUBSCRIBE_REQ",
		"handle_cdc_subscribe_stream",
		"cdc_registry: Arc<CdcSubscriptionRegistry>",
		"cdc_live_tail_rings",
		"cdc_locality_frontiers: Arc<CdcLocalityFrontiers>",
		"cdc_pending_live_entries: Arc<DashMap<PendingFinalizationId, Vec<CdcStagedEntryRef>>>",
		"cdc_marker_pending_live_entries: Arc<DashMap<CdcFinalizationKey, Vec<CdcStagedEntryRef>>>",
		"struct CdcFinalizationKey",
		"fn from_table_replica_meta(meta: &TableReplicaWriteMeta) -> Self",
		"spawn_cdc_finalization_release_observer",
		"cdc_release_pending_finalization_marker",
		"stage_cdc_table_replica_children",
		"self.cdc_stage_pending_finalization_marker",
		"CdcFinalizationKey::from_table_replica_meta(&meta)",
		"cdc_heartbeat_localities: Arc<DashMap<Locality, ()>>",
		"cdc_heartbeat_observed_frontiers: Arc<DashMap<Locality, HybridTimestamp>>",
		"stage_cdc_table_children",
		".with_finalized(finalized)",
		"self.cdc_pending_live_entries",
		"self.cdc_release_pending_finalization(record.id).await",
		"self.cdc_evict_pending_finalization(pending.id).await",
		"ring.mark_finalized_entry",
		"} else {\n\t\t\t\tmetrics().observe_cdc_pending_finalization_evicted();",
		"metrics().observe_cdc_pending_finalization_evicted()",
		"cdc_emit_frame",
		"collect_cdc_catchup_events",
		"compute_cdc_visible_row_keys_at_cursor",
		"cdc_open_time_ring_continuity_check",
		"cdc_safe_emission_hlc",
		"cdc_capture_snapshot_hlc",
		"cdc_live_retention_reject_code",
		"cdc_live_recovery_upper_after",
		"evicted_cursor_frontier",
		"emit_cdc_live_recovery_catchup_events",
		"handle.cursor_high_water.advance_max(recovery_upper)",
		"CdcFrameSource::Catchup.as_raw()",
		"cdc_retention_reject_code_for_cursor",
		"cdc_table_maybe_replicated_here",
		"cdc_ranges_maybe_replicated_by_local_node",
		"CdcRejectCode::TableNotReplicatedHere",
		"emit_cdc_subscription_opened_audit",
		"emit_cdc_subscription_closed_audit",
		"AuditOperation::CdcSubscriptionOpened",
		"AuditOperation::CdcSubscriptionClosed",
		"cdc_cursor_audit_json",
		"cdc_reject_audit_reason",
		"cdc_min_locality_frontier_for_ranges",
		"cdc_source_localities_for_ranges",
		"CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET",
		"cdc_source_localities_from_ranges_and_rings",
		"register_cdc_heartbeat_localities",
		"register_cdc_heartbeat_localities_for_primary_ranges",
		"cdc_heartbeat_localities_for_primary_ranges",
		"ring.primary_ranges_for(self.local_node)",
		"self.register_cdc_heartbeat_locality(child.locality);",
		"spawn_cdc_locality_heartbeat_supervisor",
		"CdcLocalityHeartbeatWorker::new(locality_id, interval)",
		"cdc_locality_quiet_since_last_finalization",
		"emit_cdc_heartbeat_finalization_marker",
		"cdc_heartbeat_identity",
		"cdc_heartbeat_payload_hash",
		"WendelonRequest::Finalize(finalization)",
		"tenant_id: TenantId::ZERO",
		"charged_bytes: 0",
		"record_count: 0",
		"localities.insert(range.start)",
		"localities.insert(range.end)",
		"cdc_locality_frontiers.wait_for_advance",
		"drain_cdc_ready_live_entries",
		"pending_live_entries.extend",
		"let catchup_started = Instant::now()",
		"metrics().observe_cdc_catchup_to_live_handoff(catchup_started.elapsed())",
		"handle.cursor_high_water.load()",
		"pending.dedup_by_key(|entry| entry.cursor)",
		"CdcRejectCode::BelowRetentionFloor",
		"self.cdc_registry.remove",
		"cdc_snapshot_hlc_from_state",
		"check_read_against_retention(req.start_cursor.row_hlc",
		"cdc_emit_checkpoint",
		"CDC_CHECKPOINT_EVENT_CADENCE",
		"CDC_CHECKPOINT_WALL_CADENCE",
		"CdcCheckpointCadence",
		"checkpoint_cadence.note_events(meta.event_count.get())",
		"checkpoint_cadence.checkpoint_due(now)",
		"cdc_emit_checkpoint(stream, stream_id, &checkpoint, CdcFrameSource::Ring)",
		"storage_row_is_tombstone_at",
		"cdc_tombstone_event_batch",
		"CatchupSnapshot::new(self.cdc_capture_snapshot_hlc(&source_ranges).await)",
	] {
		if !data_server.contains(required) {
			violations.push(format!(
				"src/network/data_server.rs: missing ADR 054 integration token {required}"
			));
		}
	}
	let cdc_handler_section = data_server
		.split("async fn handle_cdc_subscribe_stream")
		.nth(1)
		.and_then(|tail| tail.split("async fn emit_cdc_catchup_events").next())
		.unwrap_or("");
	if cdc_handler_section.contains("cdc_snapshot_hlc_from_state(&state)") {
		violations.push(
			"src/network/data_server.rs: CDC subscribe open must capture snapshot through cdc_capture_snapshot_hlc after source_ranges, not directly from state".to_string(),
		);
	}

	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	for required in [
		"DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS",
		"cdc_finalization_release_observer: JoinHandle<()>",
		"cdc_locality_heartbeat: JoinHandle<()>",
		"spawn_cdc_finalization_release_observer()",
		"spawn_cdc_locality_heartbeat_supervisor(\n\t\t\tring.clone()",
		"self.cdc_finalization_release_observer.abort()",
		"self.cdc_locality_heartbeat.abort()",
		"await_background_task(\n\t\t\tself.cdc_finalization_release_observer",
		"await_background_task(self.cdc_locality_heartbeat, \"CDC locality heartbeat\")",
	] {
		if !node.contains(required) {
			violations.push(format!(
				"src/node.rs: missing ADR 054 heartbeat supervisor token {required}"
			));
		}
	}

	let storage_cdc = fs::read_to_string(manifest_dir.join("src/storage/cdc.rs")).unwrap();
	for required in [
		"DEFAULT_MAX_CDC_LIVE_TAIL_BYTES_PER_LOCALITY",
		"DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS",
		"pub struct CdcLiveTailRing",
		"pub struct CdcLiveEntry",
		"mark_finalized_entry",
		"eviction_frontiers: HashMap<(TenantId, TableObjectId), CdcCursor>",
		"pub async fn evicted_cursor_frontier",
		"pub struct CdcLocalityFrontiers",
		"min_for_localities",
		"wait_for_advance",
		"notify_waiters",
		"CdcFinalizationObserver",
		"open_time_ring_continuity_check",
		"pub struct CdcLocalityHeartbeatWorker",
		"pub locality_id: Locality",
	] {
		if !storage_cdc.contains(required) {
			violations.push(format!(
				"src/storage/cdc.rs: missing ADR 054 token {required}"
			));
		}
	}
	let heartbeat_section = storage_cdc
		.split("pub struct CdcLocalityHeartbeatWorker")
		.nth(1)
		.unwrap_or("");
	if heartbeat_section.contains("table_object_id") {
		violations.push(
			"src/storage/cdc.rs: heartbeat worker must not carry table_object_id".to_string(),
		);
	}

	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for required in [
		"cdc_event",
		"wendelon_cdc_subscriptions_total",
		"wendelon_cdc_events_emitted_total",
		"wendelon_cdc_event_bytes_emitted_total",
		"wendelon_cdc_checkpoints_emitted_total",
		"wendelon_cdc_rejects_total",
		"wendelon_cdc_subscription_lag_micros",
		"wendelon_cdc_ring_overflow_total",
		"wendelon_cdc_catchup_to_live_handoff_micros",
		"wendelon_cdc_staged_entries_in_ring",
		"wendelon_cdc_pending_finalization_evicted_total",
		"below_retention_floor",
		"ring_overflow_during_catchup",
		"schema_fingerprint_incompatible",
		"checkpoint_digest_mismatch",
		"unknown_table",
		"tenant_auth_denied",
		"table_not_replicated_here",
		"internal_engine_error",
	] {
		if !metrics.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 054 metric token {required}"
			));
		}
	}
	for forbidden in [
		"cdc_.*tenant_id",
		"cdc_.*table_object_id",
		"subscription_id\"",
		"schema_fingerprint\"",
	] {
		if metrics.contains(forbidden) {
			violations.push(format!(
				"src/metrics/registry.rs: contains forbidden CDC metric label token {forbidden}"
			));
		}
	}

	let runbook = manifest_dir.join("plans/ops/runbooks/cdc_subscription.md");
	if !runbook.exists() {
		violations.push("plans/ops/runbooks/cdc_subscription.md is missing".to_string());
	} else {
		let runbook = fs::read_to_string(&runbook).unwrap();
		for required in [
			"## Promotion Evidence",
			"scripts/bench/run_cdc_subscription_acceptance.sh",
			"measured_one_way_quic_rtt_micros",
			"promotion_candidate=true",
			"cdc_acceptance_*",
			"placeholder RTT",
		] {
			if !runbook.contains(required) {
				violations.push(format!(
					"plans/ops/runbooks/cdc_subscription.md: missing ADR 054 promotion-evidence token {required}"
				));
			}
		}
	}
	let runbook_index =
		fs::read_to_string(manifest_dir.join("plans/ops/runbooks/README.md")).unwrap();
	if !runbook_index.contains("cdc_subscription.md") {
		violations.push("plans/ops/runbooks/README.md: CDC runbook is not cataloged".to_string());
	}
	let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	if !cargo_toml.contains("name = \"cdc_subscription\"") {
		violations.push("Cargo.toml: ADR 054 CDC subscription benchmark is missing".to_string());
	}
	let bench_path = manifest_dir.join("benches/cdc_subscription.rs");
	if !bench_path.exists() {
		violations
			.push("benches/cdc_subscription.rs: ADR 054 bench harness is missing".to_string());
	} else {
		let bench = fs::read_to_string(bench_path).unwrap();
		for required in [
			"FULL_MATRIX",
			"--full",
			"--acceptance-one-way-quic-rtt-micros",
			"cdc_live_tail_latency",
			"cdc_live_tail_fanout",
			"cdc_catchup_throughput",
			"cdc_ring_overflow_threshold",
			"cdc_admission_isolation",
			"cdc_stage_release_drain",
			"cdc_safe_emission_watermark_hold",
			"cdc_acceptance_live_tail_latency",
			"cdc_acceptance_catchup_throughput",
			"cdc_acceptance_ring_overflow_nonblocking",
			"cdc_acceptance_admission_isolation",
			"CDC acceptance thresholds failed",
		] {
			if !bench.contains(required) {
				violations.push(format!(
					"benches/cdc_subscription.rs: missing ADR 054 bench token {required}"
				));
			}
		}
	}
	let bench_results = fs::read_to_string(
		manifest_dir.join("plans/impl/054_cdc_subscription_substrate_bench_results.md"),
	)
	.unwrap();
	for required in [
		"--acceptance-one-way-quic-rtt-micros",
		"cdc_acceptance_live_tail_latency",
		"cdc_acceptance_catchup_throughput",
		"cdc_acceptance_ring_overflow_nonblocking",
		"cdc_acceptance_admission_isolation",
		"representative deployment evidence",
		"Representative Acceptance Template",
		"measured one-way QUIC RTT micros",
		"CPU governor / power profile",
		"scripts/bench/run_cdc_subscription_acceptance.sh",
		"promotion_candidate=true",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/054_cdc_subscription_substrate_bench_results.md: missing ADR 054 bench-results token {required}"
			));
		}
	}
	let acceptance_script = manifest_dir.join("scripts/bench/run_cdc_subscription_acceptance.sh");
	if !acceptance_script.exists() {
		violations.push("scripts/bench/run_cdc_subscription_acceptance.sh is missing".to_string());
	} else {
		let script = fs::read_to_string(acceptance_script).unwrap();
		for required in [
			"CDC_ACCEPTANCE_BENCH_ARGS",
			"CDC_ACCEPTANCE_MATRIX_ROWS",
			"--acceptance-one-way-quic-rtt-micros",
			"cdc_subscription_acceptance.log",
			"validation_status=pass",
			"promotion_candidate=true",
			"cdc_acceptance_live_tail_latency",
			"cdc_acceptance_catchup_throughput",
			"cdc_acceptance_ring_overflow_nonblocking",
			"cdc_acceptance_admission_isolation",
		] {
			if !script.contains(required) {
				violations.push(format!(
					"scripts/bench/run_cdc_subscription_acceptance.sh: missing ADR 054 acceptance token {required}"
				));
			}
		}
	}
	let observability =
		fs::read_to_string(manifest_dir.join("plans/ops/observability.md")).unwrap();
	for required in [
		"#### Change Data Capture",
		"wendelon_cdc_subscriptions_total",
		"wendelon_cdc_events_emitted_total",
		"wendelon_cdc_event_bytes_emitted_total",
		"wendelon_cdc_checkpoints_emitted_total",
		"wendelon_cdc_rejects_total",
		"wendelon_cdc_subscription_lag_micros",
		"wendelon_cdc_ring_overflow_total",
		"wendelon_cdc_catchup_to_live_handoff_micros",
		"wendelon_cdc_staged_entries_in_ring",
		"wendelon_cdc_pending_finalization_evicted_total",
		"CDC metrics deliberately do not label tenant, table, subscription id",
	] {
		if !observability.contains(required) {
			violations.push(format!(
				"plans/ops/observability.md: missing ADR 054 CDC metric token {required}"
			));
		}
	}
	let fixtures = manifest_dir.join("tests/fixtures/cdc_subscription/README.md");
	if !fixtures.exists() {
		violations.push("tests/fixtures/cdc_subscription/README.md is missing".to_string());
	} else {
		let fixtures = fs::read_to_string(fixtures).unwrap();
		for required in [
			"same_hlc_suffix_catchup",
			"live_tail_release",
			"open_time_ring_continuity",
			"checkpoint_visible_digest",
			"reject_taxonomy",
		] {
			if !fixtures.contains(required) {
				violations.push(format!(
					"tests/fixtures/cdc_subscription/README.md: missing fixture case {required}"
				));
			}
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 054 CDC subscription invariants violated:\n{}",
		violations.join("\n")
	);
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

#[test]
fn test_adr056_row_group_fetch_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let row_group = fs::read_to_string(manifest_dir.join("src/storage/row_group.rs")).unwrap();
	let columnar = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	let multirow = fs::read_to_string(manifest_dir.join("src/storage/multirow.rs")).unwrap();
	let manager = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let cargo = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let transport = fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let cdc = fs::read_to_string(manifest_dir.join("src/storage/cdc.rs")).unwrap();

	let mut violations = Vec::new();

	for unit in [
		"impl_unit!(IpcColumnOrdinal, u32);",
		"impl_unit!(RowGroupIndex, u32);",
		"impl_unit!(RowGroupRowCount, u32);",
	] {
		if !units.contains(unit) {
			violations.push(format!("src/schema/units.rs: missing ADR 056 unit {unit}"));
		}
	}
	for definition in [
		"pub struct RecordPageDirectory",
		"pub struct RowGroupDirectoryEntry",
		"pub struct BufferPageDirectoryEntry",
		"pub struct RowGroupColumnStats",
		"pub struct RowGroupFetchPlan",
		"pub struct PhaseBOutcome",
	] {
		if row_group.matches(definition).count() != 1 {
			violations.push(format!(
				"src/storage/row_group.rs: expected exactly one {definition}"
			));
		}
	}
	for required in [
		"pub enum RowGroupRejectCode",
		"PageDirectoryParseError = 1",
		"RowGroupRangeOverflow = 2",
		"LayoutUnsupported = 3",
		"pub enum RowGroupFallbackReason",
		"TooFewRows",
		"TooManyRowGroups",
		"UnpagedRecord",
		"pub struct RowGroupWriterConfig",
		"pub struct L2RowGroupFetchConfig",
		"storage.row_group_directory",
		"phase_b_fetch",
		"phase_c_fetch",
		"logical_schema_fingerprint",
		"predicate_fetched_groups",
		"apply_phase_b_masks_to_projected_row_groups",
	] {
		if !row_group.contains(required) {
			violations.push(format!(
				"src/storage/row_group.rs: missing ADR 056 token {required}"
			));
		}
	}
	if !columnar.contains("RowGroup(RowGroupFetchPlan)") {
		violations.push("src/storage/columnar.rs: L2FetchStrategy::RowGroup missing".to_string());
	}
	if !columnar.contains("page_directory: Option<RecordPageDirectory>") {
		violations.push(
			"src/storage/columnar.rs: RecordBufferDirectory page_directory missing".to_string(),
		);
	}
	if !multirow.contains("RowGroupWriterConfig")
		|| !multirow.contains("encode_record_batches_bytes")
	{
		violations.push("src/storage/multirow.rs: row-group writer path is not wired".to_string());
	}
	for metric in [
		"storage_l2_row_group_fetch_requests_total",
		"storage_l2_row_group_fetch_fallback_total",
		"storage_l2_row_group_fetch_row_groups_total",
		"storage_l2_row_group_fetch_bytes_total",
		"storage_l2_row_group_fetch_plan_micros",
		"storage_l2_row_group_fetch_selected_ranges",
		"storage_l2_row_group_fetch_directory_bytes",
		"storage_l2_row_group_fetch_invariant_violations_total",
		"violation_kind",
	] {
		if !metrics.contains(metric) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 056 metric token {metric}"
			));
		}
	}
	for forbidden in [
		"__row_group_directory__",
		"SyntheticColumn::RowGroupDirectory",
		"partial_buffer_materialize",
		"slice_compressed_buffer",
		"intra_buffer_page_decode",
		"mini_page_",
	] {
		for (path, content) in [
			("src/storage/row_group.rs", &row_group),
			("src/storage/columnar.rs", &columnar),
			("src/storage/multirow.rs", &multirow),
		] {
			if content.contains(forbidden) {
				violations.push(format!("{path}: forbidden ADR 056 V1 token {forbidden}"));
			}
		}
	}
	for forbidden in [
		"MsgType::ROW_GROUP",
		"MsgType::PAGE_DIRECTORY",
		"FRAME_FLAG_ROW_GROUP",
		"FRAME_FLAG_PAGE_DIRECTORY",
	] {
		if schema_core.contains(forbidden) {
			violations.push(format!(
				"src/schema/core.rs: forbidden ADR 056 token {forbidden}"
			));
		}
	}
	for forbidden in [
		"NodeCapability::RowGroup",
		"NodeCapability::PageDirectory",
		"NodeCapability::ROW_GROUP",
		"NodeCapability::PAGE_DIRECTORY",
	] {
		if transport.contains(forbidden) {
			violations.push(format!(
				"src/network/transport.rs: forbidden ADR 056 capability {forbidden}"
			));
		}
	}
	if cdc.contains("RecordPageDirectory") || cdc.contains("RowGroupFetchPlan") {
		violations.push("src/storage/cdc.rs: CDC must not import ADR 056 internals".to_string());
	}
	for token in [
		"RowGroupFetchSampled",
		"row_group.fetch_sampled",
		"RowGroupInvariantViolation",
		"row_group.invariant_violation",
	] {
		if !audit.contains(token) {
			violations.push(format!("src/audit.rs: missing ADR 056 audit token {token}"));
		}
	}
	for token in [
		"emit_row_group_fetch_sampled_audit",
		"emit_row_group_invariant_audit_for_error",
		"AuditOperation::RowGroupFetchSampled",
		"AuditOperation::RowGroupInvariantViolation",
		"row_groups_fetched",
		"predicate_fetched_groups",
		"dominant_strategy",
	] {
		if !data_server.contains(token) {
			violations.push(format!(
				"src/network/data_server.rs: missing ADR 056 audit emission token {token}"
			));
		}
	}
	if data_server
		.matches("emit_row_group_invariant_audit_for_error(")
		.count()
		< 2
	{
		violations.push(
			"src/network/data_server.rs: RowGroupInvariantViolation audit helper must have at least one production call site"
				.to_string(),
		);
	}
	if !manager.contains("apply_phase_b_masks_to_projected_row_groups(") {
		violations.push(
			"src/storage/manager.rs: Phase B mask materializer must be called by production storage code"
				.to_string(),
		);
	}
	if !columnar.contains("observe_storage_l2_row_group_fetch_invariant_violation(")
		&& !multirow.contains("observe_storage_l2_row_group_fetch_invariant_violation(")
	{
		violations.push(
			"row-group invariant metric must be emitted outside src/metrics/registry.rs"
				.to_string(),
		);
	}
	if row_group.contains("execute_program(") {
		violations
			.push("src/storage/row_group.rs: Phase 5 materializer must not re-run PBC".to_string());
	}
	if !cargo.contains("name = \"row_group_fetch\"") || !cargo.contains("harness = false") {
		violations.push("Cargo.toml: ADR 056 row_group_fetch bench is not registered".to_string());
	}
	let bench_path = manifest_dir.join("benches/row_group_fetch.rs");
	if !bench_path.exists() {
		violations.push("benches/row_group_fetch.rs: ADR 056 bench harness is missing".to_string());
	} else {
		let bench = fs::read_to_string(bench_path).unwrap();
		for token in [
			"writer",
			"planner_one_group",
			"phase_b_mask",
			"masked_materialization",
			"--quick",
		] {
			if !bench.contains(token) {
				violations.push(format!(
					"benches/row_group_fetch.rs: missing ADR 056 bench token {token}"
				));
			}
		}
	}
	for forbidden in [
		"RowGroupV1",
		"RowGroupV2",
		"PageDirectoryV1",
		"PageDirectoryV2",
		"WendelonRowGroup",
		"WendelonPageDirectory",
	] {
		if row_group.contains(forbidden)
			|| columnar.contains(forbidden)
			|| multirow.contains(forbidden)
		{
			violations.push(format!("ADR 056 forbidden suffix/prefix token {forbidden}"));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 056 row-group fetch invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr057_encoding_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let encoding_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let encoding_directory =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/directory.rs")).unwrap();
	let encoding_materialize =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/materialize.rs")).unwrap();
	let encoding_select =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/select.rs")).unwrap();
	let encoding_sample =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/sample.rs")).unwrap();
	let columnar = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	let multirow = fs::read_to_string(manifest_dir.join("src/storage/multirow.rs")).unwrap();
	let row_group = fs::read_to_string(manifest_dir.join("src/storage/row_group.rs")).unwrap();
	let manager = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let cargo = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let transport = fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let cdc = fs::read_to_string(manifest_dir.join("src/storage/cdc.rs")).unwrap();

	let mut violations = Vec::new();

	for unit in [
		"impl_unit!(BitsPerValue, u8);",
		"impl_unit!(BaseValue, i128);",
		"impl_unit!(RunCount, u32);",
		"impl_unit!(DictValueCount, u32);",
		"impl_unit!(Exponent, i16);",
		"impl_unit!(Factor, i64);",
		"impl_unit!(ExceptionCount, u32);",
		"impl_unit!(EncodingPageIndex, u32);",
		"impl_unit!(EncodingPageByteOffset, u32);",
		"impl_unit!(EncodingCascadeIndex, u16);",
		"impl_unit!(BufferValueIndex, u32);",
		"impl_unit!(BufferValueCount, u32);",
		"impl_unit!(SimdPageValueCount, u32);",
		"impl_unit!(SampleRowCount, u32);",
		"impl_unit!(EncodingPageCount, u32);",
		"impl_unit!(TaskCount, u32);",
	] {
		if !units.contains(unit) {
			violations.push(format!("src/schema/units.rs: missing ADR 057 unit {unit}"));
		}
	}

	for definition in [
		"pub enum EncodingCascadeKind",
		"pub enum EncodingLeafCascade",
		"pub enum EncodingCascade {",
		"pub struct EncodingDirectory",
		"pub struct BufferEncoding",
		"pub struct EncodingPageDescriptor",
		"pub enum EncodingRejectCode",
		"pub enum EncodingFallbackReason",
		"pub struct EncodingWriterConfig",
		"pub struct L2EncodingFetchConfig",
	] {
		if encoding_mod.matches(definition).count() != 1 {
			violations.push(format!(
				"src/storage/encoding/types.rs: expected exactly one {definition}"
			));
		}
	}
	for required in [
		"Plain = 1",
		"Lz4Frame = 2",
		"BitPacked = 3",
		"Rle = 4",
		"Dictionary = 5",
		"Delta = 6",
		"Fsst = 7",
		"Alp = 8",
		"pub const ALL: [Self; 8]",
		"storage.encoding_directory",
		"cascades: Vec<EncodingCascade>",
		"cascade_index: EncodingCascadeIndex",
		"page_end_offsets: Vec<EncodingPageByteOffset>",
		"EncodingDirectoryParseError = 1",
		"BufferEncodingMismatch = 2",
		"PageRangeOverflow = 3",
		"CascadeUnsupported = 4",
		"DecodedLengthMismatch = 5",
		"enabled_cascades: BTreeSet<EncodingCascadeKind>",
	] {
		if !encoding_mod.contains(required) {
			violations.push(format!(
				"src/storage/encoding/types.rs: missing ADR 057 token {required}"
			));
		}
	}
	if encoding_mod.contains("Reserved") {
		violations.push(
			"src/storage/encoding/types.rs: cascade catalog must not reserve variants".into(),
		);
	}
	for required in [
		"build_encoding_directory_for_ipc_stream",
		"serialize_encoding_directory_value",
		"deserialize_encoding_directory_value",
		"validate_encoding_directory",
		"apply_encoding_buffers_in_place",
		"page_end_offsets",
		"seen.insert((",
	] {
		if !encoding_directory.contains(required) {
			violations.push(format!(
				"src/storage/encoding/directory.rs: missing ADR 057 token {required}"
			));
		}
	}
	for forbidden in [
		"Vec<EncodingSimdPageEntry>",
		"pub struct EncodingSimdPageEntry",
		"simd_pages:",
		"per_page_rows",
	] {
		if encoding_directory.contains(forbidden) || encoding_mod.contains(forbidden) {
			violations.push(format!(
				"ADR 057 compact directory forbidden token {forbidden}"
			));
		}
	}
	for module in [
		"plain.rs",
		"lz4.rs",
		"bitpacked.rs",
		"rle.rs",
		"dict.rs",
		"delta.rs",
		"fsst.rs",
		"alp.rs",
	] {
		if !manifest_dir
			.join("src/storage/encoding/cascade")
			.join(module)
			.exists()
		{
			violations.push(format!("src/storage/encoding/cascade/{module}: missing"));
		}
	}
	for required in [
		"select_and_encode_buffer",
		"cascade_enabled",
		"sample_buffer",
		"cascade_sample_rows",
		"observe_storage_l2_encoding_cascade_select",
	] {
		if !encoding_select.contains(required)
			&& !encoding_sample.contains(required)
			&& !encoding_directory.contains(required)
		{
			violations.push(format!(
				"src/storage/encoding/select.rs: missing ADR 057 selection token {required}"
			));
		}
	}
	for required in [
		"pub struct ColumnSample",
		"deterministic_sample_indices",
		"distinct_count_estimate",
		"run_count_estimate",
		"is_monotonic",
		"fsst_compressible",
		"alp_candidate",
	] {
		if !encoding_sample.contains(required) {
			violations.push(format!(
				"src/storage/encoding/sample.rs: missing ADR 057 sample token {required}"
			));
		}
	}
	for required in [
		"materialize_encoding_buffer",
		"materialize_encoding_ipc_stream",
		"find_buffer_encoding",
		"row_group_index",
		"observe_storage_l2_encoding_materialize",
	] {
		if !encoding_materialize.contains(required) {
			violations.push(format!(
				"src/storage/encoding/materialize.rs: missing ADR 057 materializer token {required}"
			));
		}
	}
	for required in [
		"encoding_directory: Option<EncodingDirectory>",
		"EncodingRejected(EncodingRejectCode)",
		"ENCODING_DIRECTORY_METADATA_KEY",
		"validate_encoding_directory",
		"materialize_encoding_buffer",
		"encoding_for_ipc_body_buffer",
	] {
		if !columnar.contains(required) {
			violations.push(format!(
				"src/storage/columnar.rs: missing ADR 057 columnar token {required}"
			));
		}
	}
	for required in [
		"encode_multirow_from_survivors_with_row_group_and_encoding_config",
		"build_encoding_directory_for_ipc_stream",
		"compact_encoding_ipc_stream",
		"serialize_encoding_directory_value",
		"materialize_encoding_ipc_stream",
	] {
		if !multirow.contains(required) {
			violations.push(format!(
				"src/storage/multirow.rs: missing ADR 057 writer token {required}"
			));
		}
	}
	if !manager.contains("pack_multirow_source_rows_with_policy_and_row_group_and_encoding_config")
		|| !manager.contains("&self.config.encoding_writer")
	{
		violations.push(
			"src/storage/manager.rs: compaction/admission paths must pass Encoding writer config"
				.to_string(),
		);
	}
	for required in [
		"!self.config.l2_encoding_fetch.enabled",
		"observe_storage_l2_encoding_fallback(\"reader_disabled\")",
		"try_read_encoding_indexed_pages_from_l2",
		"build_encoding_indexed_page_plan",
		"materialize_encoding_indexed_pages",
		"indexed_rows_materialized",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 057 L2 Encoding reader gate token {required}"
			));
		}
	}
	if !row_group.contains("encoding_fetch_range_for_buffer_page")
		|| !row_group.contains("observe_storage_l2_encoding_simd_pages_fetched")
	{
		violations.push(
			"src/storage/row_group.rs: selected row-group ranges must narrow to Encoding encoded_range"
				.to_string(),
		);
	}
	for required in [
		"encoding_writer: RawEncodingWriterConfig",
		"l2_encoding_fetch: RawL2EncodingFetchConfig",
		"enabled_cascades",
		"target_simd_page_values",
		"max_parallel_materialize_tasks",
	] {
		if !config.contains(required) {
			violations.push(format!(
				"src/config.rs: missing ADR 057 config token {required}"
			));
		}
	}
	if !node.contains("pub encoding_writer: EncodingWriterConfig")
		|| !node.contains("pub l2_encoding_fetch: L2EncodingFetchConfig")
	{
		violations.push("src/node.rs: NodeBootstrapConfig missing ADR 057 configs".to_string());
	}
	for metric in [
		"storage_l2_encoding_buffer_encodings_total",
		"storage_l2_encoding_materialize_micros",
		"storage_l2_encoding_simd_pages_fetched_total",
		"storage_l2_encoding_compression_ratio",
		"storage_l2_encoding_cascade_select_micros",
		"storage_l2_encoding_directory_bytes",
		"storage_l2_encoding_fallback_total",
		"storage_l2_encoding_bytes_total",
		"storage_l2_encoding_invariant_violations_total",
		"cascade",
		"reason",
		"violation_kind",
		"EncodingCascadeHistograms",
	] {
		if !metrics.contains(metric) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 057 metric token {metric}"
			));
		}
	}
	for required in [
		"EncodingMaterializeSampled",
		"encoding.materialize_sampled",
		"EncodingInvariantViolation",
		"encoding.invariant_violation",
	] {
		if !audit.contains(required) {
			violations.push(format!(
				"src/audit.rs: missing ADR 057 audit token {required}"
			));
		}
	}
	for required in [
		"emit_encoding_materialize_sampled_audit",
		"AuditOperation::EncodingMaterializeSampled",
		"emit_encoding_invariant_audit_for_error",
		"encoding_invariant_violation_kind",
		"AuditOperation::EncodingInvariantViolation",
	] {
		if !data_server.contains(required) {
			violations.push(format!(
				"src/network/data_server.rs: missing ADR 057 audit token {required}"
			));
		}
	}
	if data_server
		.matches("emit_encoding_materialize_sampled_audit(")
		.count()
		< 2
	{
		violations.push(
			"src/network/data_server.rs: EncodingMaterializeSampled audit helper must have at least one production call site"
				.to_string(),
		);
	}
	if let Some(execute_aggregate_local) =
		data_server.split("async fn execute_aggregate_local").nth(1)
	{
		let csb_pos = execute_aggregate_local.find(".try_aggregate_csb_direct_answer");
		let projected_pos = execute_aggregate_local.find("aggregate_projected_execution");
		let plan_pos = execute_aggregate_local.find("plan_query_execution_for_ranges_with_program");
		match (csb_pos, projected_pos, plan_pos) {
			(Some(csb_pos), Some(projected_pos), Some(plan_pos))
				if csb_pos < projected_pos && csb_pos < plan_pos => {}
			_ => violations.push(
				"src/network/data_server.rs: aggregate CSB direct answer must bypass projected-read and Encoding planner paths"
					.to_string(),
			),
		}
	} else {
		violations.push(
			"src/network/data_server.rs: missing execute_aggregate_local for CSB/Encoding ordering guard"
				.to_string(),
		);
	}
	if !cargo.contains("name = \"encoding\"") || !cargo.contains("harness = false") {
		violations.push("Cargo.toml: ADR 057 encoding bench is not registered".to_string());
	}
	for test_file in [
		"encoding_cascade.rs",
		"encoding_select.rs",
		"encoding_directory.rs",
		"encoding_materialize.rs",
		"encoding_simd_page.rs",
		"encoding_planner.rs",
		"encoding_secondary_index.rs",
		"encoding_lsm.rs",
		"encoding_neutral.rs",
	] {
		if !manifest_dir.join("tests").join(test_file).exists() {
			violations.push(format!("tests/{test_file}: ADR 057 test file is missing"));
		}
	}
	let bench_path = manifest_dir.join("benches/encoding.rs");
	if !bench_path.exists() {
		violations.push("benches/encoding.rs: ADR 057 bench harness is missing".to_string());
	} else {
		let bench = fs::read_to_string(bench_path).unwrap();
		for token in [
			"writer_encoding_enabled",
			"full_materialize",
			"cascade_encode",
			"cascade_decode",
			"directory_size",
			"--quick",
		] {
			if !bench.contains(token) {
				violations.push(format!(
					"benches/encoding.rs: missing ADR 057 bench token {token}"
				));
			}
		}
	}
	if !manifest_dir.join("plans/ops/runbooks/encoding.md").exists() {
		violations.push("plans/ops/runbooks/encoding.md: ADR 057 runbook is missing".to_string());
	}
	for forbidden in [
		"MsgType::Encoding",
		"MsgType::EncodingCascade",
		"FRAME_FLAG_ENCODING",
		"FRAME_FLAG_ENCODING_CASCADE",
		"FRAME_FLAG_SIMD_PAGE",
	] {
		if schema_core.contains(forbidden) {
			violations.push(format!(
				"src/schema/core.rs: forbidden ADR 057 wire token {forbidden}"
			));
		}
	}
	for forbidden in [
		"NodeCapability::Encoding",
		"NodeCapability::EncodingDirectory",
		"NodeCapability::EncodingCascade",
		"NodeCapability::SimdPage",
	] {
		if transport.contains(forbidden) {
			violations.push(format!(
				"src/network/transport.rs: forbidden ADR 057 capability {forbidden}"
			));
		}
	}
	let cdc_identifiers = rust_identifiers_from_source("src/storage/cdc.rs", &cdc);
	if has_rust_identifier(&cdc_identifiers, "EncodingCascade")
		|| has_rust_identifier(&cdc_identifiers, "EncodingDirectory")
	{
		violations.push("src/storage/cdc.rs: CDC must not import ADR 057 internals".to_string());
	}

	assert!(
		violations.is_empty(),
		"ADR 057 Encoding invariants violated:\n{}",
		violations.join("\n")
	);
}
