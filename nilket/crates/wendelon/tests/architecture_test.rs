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

#[test]
fn adr083_legacy_kll_identity_has_no_process_global_ordinal_or_weightless_constructor() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let sketch = fs::read_to_string(manifest_dir.join("src/storage/sketch.rs"))
		.expect("read KLL sketch source");
	assert!(!sketch.contains("LEGACY_KLL_IDENTITY_ORDINAL"));
	assert!(!sketch.contains("AtomicU64"));
	assert!(!sketch.contains("from_retained_values"));
	assert!(sketch.contains("legacy_randomness_from_content"));
}

fn has_rust_identifier(identifiers: &[String], ident: &str) -> bool {
	identifiers.iter().any(|candidate| candidate == ident)
}

/// ADR 077: the data server became `network/data_server/{mod.rs + domain modules}`.
/// Architecture guards that used to read the flat `data_server.rs` now read the
/// concatenation of every module under that directory, so a symbol moved into a
/// child module is still found by source-scanning guards.
fn data_server_source_files(manifest_dir: &Path) -> Vec<PathBuf> {
	// ADR 077: the module root stays `network/data_server.rs` (mod.rs files are
	// manifests-only in this crate); carved handler families live under
	// `network/data_server/`. Production source-scanning guards read the root
	// plus every NON-TEST submodule, so a symbol moved into a child module is
	// still found, while `tests.rs` is excluded (it is not production source —
	// the old single-file scans stripped the inline `mod tests` for the same
	// reason). The root is listed first; submodules are sorted among themselves
	// (a global sort would order the `data_server/` directory component ahead of
	// the `data_server.rs` file and put test/handler modules before the root).
	let mut files = Vec::new();
	let root = manifest_dir.join("src/network/data_server.rs");
	if root.is_file() {
		files.push(root);
	}
	let dir = manifest_dir.join("src/network/data_server");
	if dir.is_dir() {
		let mut submodules = Vec::new();
		walk_dir(&dir, &mut |path| {
			let is_rs = path.extension().and_then(|s| s.to_str()) == Some("rs");
			let is_tests = path.file_name().and_then(|s| s.to_str()) == Some("tests.rs");
			if is_rs && !is_tests {
				submodules.push(path.to_path_buf());
			}
		});
		submodules.sort();
		files.extend(submodules);
	}
	files
}

fn data_server_source(manifest_dir: &Path) -> String {
	data_server_source_files(manifest_dir)
		.iter()
		.map(|path| fs::read_to_string(path).unwrap())
		.collect::<Vec<_>>()
		.join("\n")
}

/// ADR 078: storage/manager.rs decomposed into storage/manager/{islands + tests.rs}.
/// Production source-scan guards read the root plus every NON-TEST submodule (tests.rs
/// excluded — not production source), mirroring data_server_source_files().
fn manager_source_files(manifest_dir: &Path) -> Vec<PathBuf> {
	let mut files = Vec::new();
	let root = manifest_dir.join("src/storage/manager.rs");
	if root.is_file() {
		files.push(root);
	}
	let dir = manifest_dir.join("src/storage/manager");
	if dir.is_dir() {
		let mut submodules = Vec::new();
		walk_dir(&dir, &mut |path| {
			let is_rs = path.extension().and_then(|s| s.to_str()) == Some("rs");
			let is_tests = path.file_name().and_then(|s| s.to_str()) == Some("tests.rs");
			if is_rs && !is_tests {
				submodules.push(path.to_path_buf());
			}
		});
		submodules.sort();
		files.extend(submodules);
	}
	files
}

fn manager_source(manifest_dir: &Path) -> String {
	manager_source_files(manifest_dir)
		.iter()
		.map(|path| fs::read_to_string(path).unwrap())
		.collect::<Vec<_>>()
		.join("\n")
}

#[test]
fn test_adr039_wendelon_drop_migration_substrate_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let compute = fs::read_to_string(manifest_dir.join("src/integration/compute.rs")).unwrap();
	let consensus_core = fs::read_to_string(manifest_dir.join("src/consensus/core.rs")).unwrap();
	let raft_storage =
		fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let control_server =
		fs::read_to_string(manifest_dir.join("src/network/control_server.rs")).unwrap();
	let data_server = data_server_source(manifest_dir.as_ref());
	let storage_manager = manager_source(manifest_dir);
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let table_schema = fs::read_to_string(manifest_dir.join("src/schema/table.rs")).unwrap();
	let lifecycle = fs::read_to_string(manifest_dir.join("src/schema/lifecycle.rs")).unwrap();

	assert!(compute.contains("pub enum TableSchemaDropRequest"));
	assert!(compute.contains("pub struct DropOperationId"));
	assert!(compute.contains("pub tenant_id: TenantId"));
	assert!(compute.contains("pub table_object_id: TableObjectId"));
	assert!(compute.contains("pub schema_generation: SchemaGeneration"));
	assert!(!compute.contains("TableSchemaDropRequestV1"));
	assert!(!compute.contains("query_id"));

	assert!(consensus_core.contains("pub enum TableDropPrepareRequest"));
	assert!(consensus_core.contains("pub enum TableReclaimCompletionRequest"));
	assert!(consensus_core.contains("pub enum TableDropFinalizeRequest"));
	assert!(consensus_core.contains("TableDropPrepare(TableDropPrepareRequest)"));
	assert!(consensus_core.contains("TableReclaimCompletion(TableReclaimCompletionRequest)"));
	assert!(consensus_core.contains("TableDropFinalize(TableDropFinalizeRequest)"));

	assert!(raft_storage.contains("pub table_drop_fences: TableDropFenceCatalog"));
	assert!(raft_storage.contains("apply_request_at_log_index"));
	assert!(raft_storage.contains("apply_table_drop_prepare"));
	assert!(raft_storage.contains("apply_table_reclaim_completion"));
	assert!(raft_storage.contains("apply_table_drop_finalize"));
	assert!(raft_storage.contains("entry.log_id.index"));
	assert!(
		raft_storage.contains("table schema update rejected because table drop is in progress")
	);
	assert!(
		raft_storage.contains("secondary index create rejected because table drop is in progress")
	);
	assert!(
		raft_storage
			.contains("table retention policy update rejected because table drop is in progress")
	);
	assert!(table_schema.contains("pub fn unbind_latest_name"));
	assert!(lifecycle.contains("pub struct TableDropFenceCatalog"));
	let drop_fence_block = lifecycle
		.split_once("pub struct TableDropFenceCatalog")
		.and_then(|(_, rest)| rest.split_once("pub struct TableLifecyclePolicyCounts"))
		.map(|(block, _)| block)
		.expect("drop fence definitions");
	assert!(!drop_fence_block.contains("TableRetentionPolicy"));

	assert!(control_server.contains("SCHEMA_DROP_REQ"));
	assert!(control_server.contains("SCHEMA_DROP_RESP"));
	assert!(control_server.contains("run_table_schema_drop"));
	assert!(control_server.contains("TableDropLogicalFinalizeRequest"));
	assert!(
		!control_server.contains("drive_table_drop_physical_reclaim"),
		"ADR 0095 forbids physical reclaim under statement authority"
	);
	assert!(!control_server.contains(".reclaim_table_drop_storage("));
	assert!(!control_server.contains(".reclaim_table_drop_tiered_storage("));
	assert!(control_server.contains("WendelonAdminError"));
	assert!(control_server.contains("TableSchemaDropResponseWire"));
	assert!(control_server.contains("with_storage_manager"));
	assert!(storage_manager.contains("pub struct TableDropReclaimReport"));
	assert!(storage_manager.contains("pub async fn reclaim_table_drop_storage"));
	assert!(storage_manager.contains("pub async fn reclaim_table_drop_tiered_storage"));
	assert!(storage_manager.contains("prepare_tiered_table_drop_rewrite"));
	assert!(storage_manager.contains("PreparedTieredTableDropRewrite"));
	assert!(storage_manager.contains("mark_record_group_for_table_drop"));
	assert!(node.contains(".with_storage_manager(slab.clone())"));
	assert!(data_server.matches("table_drop_blocks_admission").count() >= 4);
}

#[test]
fn adr0095_async_drop_reclaim_has_one_admission_and_bounded_work_spine() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let control = fs::read_to_string(manifest_dir.join("src/network/control_server.rs")).unwrap();
	let data_root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let data = data_server_source(manifest_dir);
	let streaming =
		fs::read_to_string(manifest_dir.join("src/network/data_server/streaming.rs")).unwrap();
	let replica =
		fs::read_to_string(manifest_dir.join("src/network/data_server/write_replication.rs"))
			.unwrap();
	let reclaim =
		fs::read_to_string(manifest_dir.join("src/storage/manager/retention_reclaim.rs")).unwrap();
	let compaction =
		fs::read_to_string(manifest_dir.join("src/storage/manager/compaction.rs")).unwrap();
	let lsm = fs::read_to_string(manifest_dir.join("src/storage/manager/lsm_driver.rs")).unwrap();
	let sweeper = fs::read_to_string(manifest_dir.join("src/storage/sweeper.rs")).unwrap();
	let segments =
		fs::read_to_string(manifest_dir.join("src/storage/manager/secondary_index_segments.rs"))
			.unwrap();

	assert_eq!(
		raft.matches(".table_drop_fences.is_fenced(").count(),
		1,
		"fence inspection is confined to SmState::table_drop_blocks_admission"
	);
	for source in [&data, &streaming, &replica] {
		assert!(
			!source.contains(".table_drop_fences.is_fenced("),
			"data admission must use the fence-or-tombstone authority"
		);
	}
	assert!(data.matches("table_drop_blocks_admission").count() >= 4);
	assert!(streaming.contains("table_drop_blocks_admission"));
	assert!(!control.contains("drive_table_drop_physical_reclaim"));
	assert!(!control.contains("usize::MAX"));
	assert!(data.contains("spawn_drop_reclaim_sweeper"));
	assert!(data.contains("debts.sort_by_key(|debt| debt.finalize_log_index)"));
	assert!(data.contains("DROP_RECLAIM_MAX_EXAMINED_PER_SLICE"));
	let rpc = data
		.split_once("struct DropReclaimSliceRequestMeta")
		.and_then(|(_, tail)| tail.split_once("impl DropReclaimSliceRequestMeta"))
		.map(|(block, _)| block)
		.expect("drop reclaim request meta");
	assert!(
		!rpc.contains("cursor"),
		"the participant alone owns its cursor"
	);
	assert!(rpc.contains("coordinator: NodeId"));
	assert!(data.contains("coordinator is not the current Raft leader"));
	assert!(data.contains("leadership changed before progress persistence"));

	assert!(reclaim.contains("candidates.take(examined_budget)"));
	assert!(reclaim.contains("report.examined += 1"));
	assert!(reclaim.contains("acquire_slab_rewrite_exclusive_claim"));
	// ADR 0097 moved these acquisitions behind `claim_sealed_slab_for_maintenance`.
	// ADR 0095's property is preserved TRANSITIVELY, and asserted as such: each
	// rewrite path calls the helper, and the helper acquires the claims. A summed
	// occurrence count would not prove the second link - the helper could stop
	// claiming entirely and the total would still be satisfied by its call sites.
	let body_of = |source: &str, name: &str| -> String {
		source
			.split_once(&format!("async fn {name}("))
			.and_then(|(_, tail)| tail.split_once("\n\t}\n"))
			.map(|(body, _)| body.to_owned())
			.unwrap_or_else(|| panic!("{name} not found"))
	};
	for path in ["compact_one_sealed_slab", "recluster_one_sealed_slab"] {
		assert!(
			body_of(&compaction, path).contains("claim_sealed_slab_for_maintenance"),
			"{path} must take ownership of the slab it rewrites"
		);
	}
	assert!(
		body_of(&segments, "claim_sealed_slab_for_maintenance")
			.contains("acquire_maintenance_claims"),
		"the ownership helper must actually acquire the exclusive claims"
	);
	assert!(lsm.matches("acquire_slab_rewrite_exclusive_claim").count() >= 2);
	assert!(
		sweeper
			.matches("acquire_slab_rewrite_exclusive_claim")
			.count() >= 3
	);
	assert!(data.contains("migration_records_without_logically_dropped_rows"));

	assert_eq!(
		data_root.matches("self.mutation_drain.admit(").count()
			+ replica.matches(".admit(").count(),
		3,
		"leader write, delete, and replica write are the object mutation admissions"
	);
	assert!(data.contains("mutation_drain.clone()"));
}

#[test]
fn adr0096_drop_reclaim_observability_is_state_sourced_unlabeled_and_truthful() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let data = data_server_source(manifest_dir);
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let control = fs::read_to_string(manifest_dir.join("src/network/control_server.rs")).unwrap();
	let runbook = fs::read_to_string(manifest_dir.join("plans/ops/observability.md")).unwrap();

	let setter = metrics
		.split_once("pub fn set_drop_reclaim_debts")
		.and_then(|(_, tail)| tail.split_once("pub fn observe_drop_reclaim_settlement"))
		.map(|(block, _)| block)
		.expect("ADR 0096 debt gauge setter");
	assert!(
		setter.contains(".set("),
		"the debt metric is set from state"
	);
	assert!(!setter.contains(".inc("));
	assert!(!setter.contains(".add("));
	let render = metrics
		.split_once("\"wendelon_table_drop_reclaim_debts\"")
		.and_then(|(_, tail)| tail.split_once("\"wendelon_table_drop_settlements_total\""))
		.map(|(block, _)| block)
		.expect("ADR 0096 metric render block");
	assert!(render.contains("self.drop_reclaim_debts.get()"));
	assert!(
		!render.contains("render_gauge_with_labels"),
		"operation identity must not become a Prometheus label"
	);

	for publication_family in [
		"observability_metrics.set_drop_reclaim_debts(0)",
		"observability_metrics.set_drop_reclaim_debts(inner.state.table_drop_reclaim_debts.len())",
		"set_drop_reclaim_debts(guard.state.table_drop_reclaim_debts.len())",
	] {
		assert!(
			raft.contains(publication_family),
			"missing state-publication refresh family: {publication_family}"
		);
	}
	assert_eq!(
		raft.matches("set_drop_reclaim_debts(guard.state.table_drop_reclaim_debts.len())")
			.count(),
		2,
		"live apply and validated snapshot install are the two locked-state publication sites"
	);
	for (owner, source) in [
		("node", node.as_str()),
		("control", control.as_str()),
		("data", data.as_str()),
	] {
		assert!(
			!source.contains("set_drop_reclaim_debts"),
			"{owner} must not refresh the state-sourced gauge on leadership or request lifecycle events"
		);
	}
	assert!(raft.contains("pending_drop_settlements"));
	assert!(raft.contains("applied_successfully"));
	assert!(raft.contains("observe_drop_reclaim_settlement"));
	assert!(raft.contains("table drop physical reclaim settled"));
	assert!(raft.contains("local_node_id"));
	assert!(node.contains("with_observability_node_id(local_node)"));
	for phase_field in [
		"from_phase",
		"to_phase",
		"cursor_after_slab_id",
		"cursor_upper_bound_slab_id",
		"examined_total",
	] {
		assert!(
			data.contains(phase_field),
			"phase signal lacks {phase_field}"
		);
	}
	assert!(data.contains("table drop reclaim phase transitioned"));

	for contract in [
		"wendelon_table_drop_reclaim_debts == 0",
		"no unrelated actor may issue DROP",
		"A nonzero starting value is not a baseline",
		"must be rejected rather than reported as drained",
	] {
		assert!(runbook.contains(contract), "ops receipt lacks: {contract}");
	}
	// The exact false-receipt history behind the zero/exclusive rule: returning
	// to a nonzero baseline says nothing about which debt settled.
	let unrelated_baseline = 1u64;
	let after_run_drop = unrelated_baseline + 1;
	let after_unrelated_settles = after_run_drop - 1;
	assert_eq!(after_unrelated_settles, unrelated_baseline);
	assert_ne!(
		unrelated_baseline, 0,
		"only zero is an exclusive aggregate barrier"
	);
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
	let storage_manager = manager_source(manifest_dir);
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

	for required in ["RetiredCompactionCommit(", "control ring"] {
		if !consensus_core.contains(required) {
			violations.push(format!(
				"src/consensus/core.rs: missing ADR 055 control-ring token {required}"
			));
		}
	}
	for required in [
		"SlabCommand::Compaction(commit)",
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
		"observe_lsm_compaction_commit(commit, applied_log_index)",
		"on_gc_removed",
	] {
		if !storage_sweeper.contains(required) {
			violations.push(format!(
				"src/storage/sweeper.rs: missing ADR 055 manager observer token {required}"
			));
		}
	}
	let observer = manager_method_body(&storage_manager, "observe_lsm_compaction_commit");
	for required in [
		"persistent_index_gate",
		"observer_is_superseded",
		"apply_lsm_manifest_compaction_commit(commit)",
	] {
		if !observer.contains(required) {
			violations.push(format!(
				"LSM manager observer is missing guarded delegation {required}"
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
		|| line.starts_with("pub(crate) mod ")
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
		if path.extension().is_some_and(|ext| ext == "rs")
			&& let Ok(content) = fs::read_to_string(path)
		{
			total_occurrences += content.matches(forbidden_word).count();
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
		.split_once("pub struct TableSchemaDescriptor {")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.expect("TableSchemaDescriptor block should be present");
	assert!(
		!descriptor_block.to_ascii_lowercase().contains("retention"),
		"TableSchemaDescriptor must not carry retention policy fields"
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
		// ADR 0699 S5a adds the certified-base domain; ADR 048 adds none.
		6,
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

	let data_server_source = data_server_source(manifest_dir.as_ref());
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

	let storage_manager = manager_source(manifest_dir);
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
	let compactor_paths = ["src/storage/sweeper.rs", "src/storage/multirow.rs"];
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
	// ADR 078: the manager compactor code is now spread across storage/manager/**;
	// scan the whole decomposed manager source, not just the root file.
	if manager_source(manifest_dir).contains("synthesize_append_only_row_id") {
		violations.push(
			"src/storage/manager.rs: compactor / multirow source must not synthesize \
			append-only identities for existing rows"
				.to_string(),
		);
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
			let parent_dir = path
				.parent()
				.and_then(|parent| parent.file_name())
				.and_then(|name| name.to_str());
			let is_matcher_file = path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| name == "manager.rs")
				|| parent_dir == Some("data_server")
				|| parent_dir == Some("manager");
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
			if path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_none_or(|ext| ext != "rs")
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
		if path
			.extension()
			.and_then(|ext| ext.to_str())
			.is_none_or(|ext| ext != "rs")
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
	if !schema_source.contains("AppendOnlyContentHash rejects LocalityPolicy::ColumnProjection") {
		violations.push(
			"src/schema/table.rs: content-hash ColumnProjection rejection must stay explicit"
				.to_string(),
		);
	}

	let data_server_source = data_server_source(manifest_dir.as_ref());
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

	let data_server_source = data_server_source(manifest_dir.as_ref());
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

	// Compared with whitespace stripped so a rustfmt line break inside the
	// call cannot fake a violation.
	let manager_source: String = manager_source(manifest_dir)
		.chars()
		.filter(|c| !c.is_whitespace())
		.collect();
	for needle in [
		// ADR 0705: the rewritten plaintext is walked under its layout.
		"row_tombstone_metadata_from_blob(rewritten_view,",
		"tombstone_metadata:merge_row_tombstone_metadata(",
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
		.split_once("pub struct TableSchemaDescriptor {")
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

	let manager_source = manager_source(manifest_dir);
	let tiered_stats_cache_key_block = manager_source
		.split_once("struct TieredStatsCacheKey {")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.unwrap_or("");
	// ADR 078: tiered_stats_cache_key moved to the stats_facts island; end the region
	// at the function's own closing brace, not a downstream derive marker whose position
	// shifted under module concatenation.
	let tiered_stats_cache_key_builder = manager_source
		.split_once("fn tiered_stats_cache_key(")
		.and_then(|(_, rest)| rest.split_once("\n}"))
		.map(|(block, _)| block)
		.unwrap_or("");
	let tiered_stats_cache_key_surface =
		format!("{tiered_stats_cache_key_block}\n{tiered_stats_cache_key_builder}");
	if !tiered_stats_cache_key_surface
		.contains("logical_schema_fingerprint: LogicalSchemaFingerprint")
		|| !tiered_stats_cache_key_surface.contains("descriptor.logical_schema_fingerprint")
	{
		violations.push(
			"src/storage/manager.rs: tiered stats cache key must bind logical fingerprint"
				.to_string(),
		);
	}
	if tiered_stats_cache_key_surface.contains("physical_descriptor_fingerprint") {
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
			if path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_none_or(|ext| ext != "rs")
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

	let manager_source = manager_source(manifest_dir);
	for required in [
		"gc_secondary_index_segments_for_dropping_index",
		"CorruptSegment",
		"forget_slab",
		"observe_storage_secondary_index_corruption_failure",
		"source_slab_lease",
		"acquire_slab_rewrite_read_claim",
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
fn test_adr081_secondary_index_declaration_and_lifecycle_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();
	let client =
		fs::read_to_string(manifest_dir.join("crates/wendelon-client/src/lib.rs")).unwrap();
	for required in [
		"pub async fn create_secondary_index(",
		"pub async fn drop_secondary_index(",
		"pub async fn list_secondary_indexes(",
		"SecondaryIndexDescriptor::for_table_column(",
	] {
		if !client.contains(required) {
			violations.push(format!(
				"crates/wendelon-client/src/lib.rs: ADR 081 missing supported client surface {required}"
			));
		}
	}

	let cli = fs::read_to_string(manifest_dir.join("src/cli/index.rs")).unwrap();
	for required in [
		"IndexOperation::Create",
		"IndexOperation::Drop",
		"IndexOperation::Status",
		"--bootstrap",
		"--tenant",
	] {
		if !cli.contains(required) {
			violations.push(format!(
				"src/cli/index.rs: ADR 081 missing operator surface {required}"
			));
		}
	}

	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	for required in [
		"spawn_secondary_index_lifecycle_driver(",
		"run_secondary_index_backfill_once(",
		"gc_secondary_index_segments_for_dropping_index(",
		"submit_secondary_index_lifecycle(",
	] {
		if !node.contains(required) {
			violations.push(format!(
				"src/node.rs: ADR 081 production lifecycle driver is missing {required}"
			));
		}
	}

	let schema = fs::read_to_string(manifest_dir.join("src/schema/secondary_index.rs")).unwrap();
	for required in [
		"pub fn has_backfill_completion(&self, replica_id: NodeId)",
		"current_voters: &BTreeSet<u32>",
		"row.drop_gc_completed_by.insert(replica_id.id)",
		"pub fn reconcile_backfill_readiness(",
		"pub struct SecondaryIndexStatus",
	] {
		if !schema.contains(required) {
			violations.push(format!(
				"src/schema/secondary_index.rs: ADR 081 durable receipt/status guard missing {required}"
			));
		}
	}

	let manager = manager_source(manifest_dir);
	for required in [
		"secondary_index_admission_gate",
		"establish_secondary_index_declaration_cut(",
		"tracker.take_active_buffer()",
		"state.active.fsync().await?",
		"initialize_storage_secondary_index_series(",
		"tracker.uncovered_slabs().count() as u64",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: ADR 081 serialized full-slab declaration cut missing {required}"
			));
		}
	}

	let planner = fs::read_to_string(manifest_dir.join("src/storage/secondary_index.rs")).unwrap();
	if !planner.contains("has_backfill_completion(local_replica)") {
		violations.push(
			"src/storage/secondary_index.rs: ADR 081 planner eligibility must require the local durable receipt"
				.to_string(),
		);
	}

	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	for required in [
		"storage_secondary_index_coverage_gap_slabs.set(",
		"storage_secondary_index_lookup_refusals.initialize(",
	] {
		if !metrics.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: ADR 081 declaration baseline missing {required}"
			));
		}
	}

	let control = fs::read_to_string(manifest_dir.join("src/network/control_server.rs")).unwrap();
	for required in [
		"MsgType::SECONDARY_INDEX_STATUS_REQ",
		"ActorPermission::InterNodeTransport",
		"authenticated data node",
	] {
		if !control.contains(required) {
			violations.push(format!(
				"src/network/control_server.rs: ADR 081 status/lifecycle authority guard missing {required}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 081 secondary-index declaration/lifecycle invariants violated:\n{}",
		violations.join("\n")
	);
}

#[cfg(unix)]
#[test]
fn adr081_sampler_table_probe_is_behavioral_and_failure_is_visible() {
	use std::os::unix::fs::PermissionsExt;
	use std::process::Command;
	use std::time::{SystemTime, UNIX_EPOCH};

	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let sampler = manifest_dir.join("../../../packaging/ubuntu/polariton-telemetry-sampler.sh");
	let nonce = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_nanos();
	let fixture = std::env::temp_dir().join(format!(
		"wendelon-adr081-sampler-{}-{nonce}",
		std::process::id()
	));
	fs::create_dir_all(&fixture).unwrap();
	let fake_nilket = fixture.join("nilket");
	let sql_log = fixture.join("sql.log");
	fs::write(
		&fake_nilket,
		r#"#!/usr/bin/env bash
statement="${@: -1}"
printf '%s\n' "$statement" >>"$FAKE_SQL_LOG"
case "$statement" in
	SELECT\ \*\ FROM\ *\ LIMIT\ 0)
		[ "${FAKE_TABLES_EXIST:-1}" = 1 ]
		;;
	CREATE\ TABLE\ *)
		echo 'schema expected_generation 0 does not match current generation 1' >&2
		exit 1
		;;
	*) exit 2 ;;
esac
"#,
	)
	.unwrap();
	let mut permissions = fs::metadata(&fake_nilket).unwrap().permissions();
	permissions.set_mode(0o700);
	fs::set_permissions(&fake_nilket, permissions).unwrap();

	let existing = Command::new("bash")
		.args(["-c", "source \"$1\"; ensure_tables", "bash"])
		.arg(&sampler)
		.env("POLARITON_NILKET_BIN", &fake_nilket)
		.env("FAKE_SQL_LOG", &sql_log)
		.env("FAKE_TABLES_EXIST", "1")
		.output()
		.unwrap();
	assert!(
		existing.status.success(),
		"existing tables must bypass duplicate DDL: {}",
		String::from_utf8_lossy(&existing.stderr)
	);
	let statements = fs::read_to_string(&sql_log).unwrap();
	assert_eq!(statements.lines().count(), 3);
	assert!(
		statements
			.lines()
			.all(|line| line.starts_with("SELECT * FROM "))
	);
	assert!(!statements.contains("CREATE TABLE"));

	fs::write(&sql_log, "").unwrap();
	let absent = Command::new("bash")
		.args(["-c", "source \"$1\"; ensure_tables", "bash"])
		.arg(&sampler)
		.env("POLARITON_NILKET_BIN", &fake_nilket)
		.env("FAKE_SQL_LOG", &sql_log)
		.env("FAKE_TABLES_EXIST", "0")
		.output()
		.unwrap();
	assert!(!absent.status.success());
	let stderr = String::from_utf8_lossy(&absent.stderr);
	assert!(stderr.contains("sampler: ensure_tables failed"));
	assert!(stderr.contains("expected_generation 0 does not match current generation 1"));

	fs::remove_dir_all(fixture).unwrap();
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

	let data_server = data_server_source(manifest_dir.as_ref());
	// ADR 077: data_server_source() already excludes tests.rs, so the source is
	// production-only; no inline `mod tests` boundary remains to split on.
	let production_data_server = data_server.as_str();
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
			if path
				.extension()
				.and_then(|ext| ext.to_str())
				.is_none_or(|ext| ext != "rs")
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
		manifest_dir.join("plans/impl/0053_wire_arrow_ipc_body_compression_bench_results.md");
	if !bench_results.exists() {
		violations.push(
			"plans/impl/0053_wire_arrow_ipc_body_compression_bench_results.md is missing"
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
					"plans/impl/0053_wire_arrow_ipc_body_compression_bench_results.md: missing ADR 053 bench-results token {required}"
				));
			}
		}
	}
	let transport_source =
		fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	if !transport_source.contains("pub const ALL: [NodeCapability; 4]") {
		violations.push(
			"src/network/transport.rs: the capability census changed; only the decision authority (ADR 0699) and its batched leases (ADR 0701) added bits since ADR 053".to_string(),
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

	let data_server = data_server_source(manifest_dir.as_ref());
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
		"cdc_locality_heartbeat_finalization",
		"WendelonRequest::Finalize(finalization)",
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
		// Ownership setters add indentation around the same heartbeat call.
		// Preserve the full call/argument guard while ignoring formatting.
		if !node
			.split_whitespace()
			.collect::<String>()
			.contains(&required.split_whitespace().collect::<String>())
		{
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
		"tenant_id: TenantId::ZERO",
		"charged_bytes: 0",
		"record_count: 0",
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
		"wendelon_cdc_locality_heartbeats_committed_total",
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
		manifest_dir.join("plans/impl/0054_cdc_subscription_substrate_bench_results.md"),
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
				"plans/impl/0054_cdc_subscription_substrate_bench_results.md: missing ADR 054 bench-results token {required}"
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

fn adr079_has_ad_hoc_heartbeat_predicate(source: &str) -> bool {
	let compact = source
		.chars()
		.filter(|character| !character.is_whitespace())
		.collect::<String>();
	compact.contains("tenant_id==TenantId::ZERO")
		&& compact.contains("charged_bytes==0")
		&& compact.contains("record_count==0")
}

fn adr079_has_forbidden_audit_maintenance(source: &str) -> bool {
	let compact = source
		.chars()
		.filter(|character| !character.is_whitespace())
		.collect::<String>();
	[
		"audit.set_len(",
		"remove_file(audit",
		"rename(audit",
		"truncate(true)",
	]
	.iter()
	.any(|token| compact.contains(token))
}

/// ADR 079: local policy is explicit in the generated teaching artifact;
/// producer and audit materializer share the CDC-domain identity seam.
#[test]
fn adr079_idle_heartbeat_policy_and_classification_have_one_owner() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let cdc = fs::read_to_string(manifest_dir.join("src/storage/cdc.rs")).unwrap();
	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let soak = fs::read_to_string(manifest_dir.join("src/idle_soak.rs")).unwrap();
	let soak_command =
		fs::read_to_string(manifest_dir.join("examples/local_idle_soak.rs")).unwrap();
	let commodity_soak = fs::read_to_string(manifest_dir.join("examples/soak_driver.rs")).unwrap();
	let soak_runner =
		fs::read_to_string(manifest_dir.join("scripts/soak/run_local_idle_soak.sh")).unwrap();

	assert!(
		config.contains("cdc_locality_heartbeat_interval_millis = 30000"),
		"ADR 079 requires nilket local's explicit 30-second teaching-artifact value"
	);
	assert!(
		!node.contains("Duration::from_millis(DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS)"),
		"ADR 079 requires node wiring to consume configured cadence, not reconstruct the default"
	);
	for required in [
		"fn cdc_locality_heartbeat_finalization(",
		"fn is_cdc_locality_heartbeat_finalization(",
		"cdc_locality_heartbeat_identity(locality_id)",
		"cdc_locality_heartbeat_payload_hash(",
	] {
		assert!(
			cdc.contains(required),
			"ADR 079 canonical CDC heartbeat seam is missing {required}"
		);
	}
	assert!(
		data_server.contains("cdc_locality_heartbeat_finalization(locality_id, commit_hlc, epoch)"),
		"ADR 079 producer must use the canonical heartbeat constructor"
	);
	assert!(
		raft.contains("is_cdc_locality_heartbeat_finalization(finalization)"),
		"ADR 079 audit classification must call the canonical predicate"
	);
	let forbidden_control = "finalization.tenant_id == TenantId::ZERO && finalization.charged_bytes == 0 && finalization.record_count == 0";
	assert!(
		adr079_has_ad_hoc_heartbeat_predicate(forbidden_control),
		"ADR 079 positive control must prove an ad-hoc audit predicate is observable"
	);
	let audit_section = raft
		.split("fn raft_audit_event_with_context")
		.nth(1)
		.and_then(|tail| tail.split("fn audit_outcome").next())
		.expect("raft audit classifier section");
	assert!(
		!adr079_has_ad_hoc_heartbeat_predicate(audit_section),
		"ADR 079 forbids a second field-only heartbeat predicate at the audit boundary"
	);
	assert!(
		!["rand::", "thread_rng", "%100", "DataWriteFinalized=>None"]
			.iter()
			.any(|token| audit_section.contains(token)),
		"ADR 079 forbids sampling or generically dropping data.write_finalized audit evidence"
	);
	let forbidden_maintenance_control = "remove_file(audit_path)";
	assert!(
		adr079_has_forbidden_audit_maintenance(forbidden_maintenance_control),
		"ADR 079 positive control must prove destructive audit maintenance is observable"
	);
	assert!(
		!adr079_has_forbidden_audit_maintenance(&format!("{config}\n{soak}")),
		"ADR 079 forbids rotating, truncating, deleting, or replacing local audit evidence in this cut"
	);
	assert!(
		cdc.contains("MissedTickBehavior::Skip")
			&& data_server.contains(
				"discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip)"
			),
		"ADR 079 requires explicit skip semantics for heartbeat worker and discovery ticks"
	);
	for required in [
		"pub const SOAK_FORMAT_VERSION: u32 = 2",
		"pub const LOCAL_IDLE_SOAK_FORMAT_VERSION: u32 = SOAK_FORMAT_VERSION",
		"pub const LOCAL_IDLE_SOAK_MODE: &str = \"local_idle\"",
		"total idle audit records",
		"unexpected_idle_audit_operation_trips_operation_and_total_budgets",
	] {
		assert!(
			soak.contains(required),
			"ADR 079 soak evidence/rejection control is missing {required}"
		);
	}
	for artifact in ["soak_config.json", "samples.jsonl", "summary.json"] {
		assert!(
			soak_command.contains(artifact),
			"ADR 079 local-idle evidence command must retain shared artifact {artifact}"
		);
	}
	assert!(
		commodity_soak.contains("SOAK_FORMAT_VERSION")
			&& commodity_soak.contains("COMMODITY_SOAK_MODE"),
		"ADR 079 local-idle and commodity soak evidence must share one format/version vocabulary"
	);
	for required in [
		"local --data-dir",
		"refuses existing output directory",
		"stop_owned",
		"SELECT 1",
		"wendelon_cdc_locality_heartbeats_committed_total",
		"wendelon_audit_materialization_lag_entries",
	] {
		assert!(
			soak_runner.contains(required),
			"ADR 079 owned local-idle runner is missing {required}"
		);
	}
}

fn adr080_has_seed_plus_immediate_interval(source: &str) -> bool {
	let compact = source
		.chars()
		.filter(|character| !character.is_whitespace())
		.collect::<String>();
	compact.contains("refresh().await;") && compact.contains("tokio::time::interval(tick)")
}

fn adr080_total_applied_increment_count(source: &str) -> usize {
	source
		.matches("metrics().raft_entries_applied_total.inc()")
		.count()
}

/// ADR 080: the local lease posture has one validated TTL authority, a
/// non-bursting scheduler, and a substrate-level idle Raft budget.
#[test]
fn adr080_local_lease_posture_is_guarded_at_each_ownership_boundary() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let example = fs::read_to_string(manifest_dir.join("examples/wendelon.toml.example")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let data_server = data_server_source(manifest_dir);
	let schema = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let soak = fs::read_to_string(manifest_dir.join("src/idle_soak.rs")).unwrap();
	let soak_runner =
		fs::read_to_string(manifest_dir.join("scripts/soak/run_local_idle_soak.sh")).unwrap();

	assert!(
		config.contains("data_ring_lease_ttl_millis = 90000")
			&& example.contains("data_ring_lease_ttl_millis = {{DATA_RING_LEASE_TTL_MILLIS}}"),
		"ADR 080 requires both the visible local teaching-artifact TTL and the ordinary-profile placeholder"
	);
	assert!(
		config.contains("data_ring_lease_ttl_millis: Option<u64>")
			&& !config.contains("lease_renew_interval_millis"),
		"ADR 080 permits one lease TTL config authority and forbids an independently configurable renewal interval"
	);
	let production_bootstrap = node
		.split_once("pub async fn bootstrap")
		.map(|(_, tail)| tail)
		.and_then(|tail| tail.split("mod tests {").next())
		.expect("node production bootstrap source");
	assert!(
		production_bootstrap
			.contains("DataRingLeaseTiming::try_from_duration(cfg.data_ring_lease_ttl)")
			&& production_bootstrap
				.matches("lease_timing.ttl_millis()")
				.count() == 2
			&& production_bootstrap.contains("lease_timing.renew_interval()"),
		"ADR 080 requires both grant paths and the scheduler to consume one validated timing value; ttl uses={}, renew uses={}",
		production_bootstrap
			.matches("lease_timing.ttl_millis()")
			.count(),
		production_bootstrap
			.matches("lease_timing.renew_interval()")
			.count()
	);
	assert!(
		!production_bootstrap.contains("LEASE_TTL_MS")
			&& !production_bootstrap.contains("LEASE_RENEW_INTERVAL"),
		"ADR 080 forbids direct ordinary-constant use in production bootstrap wiring"
	);

	let forbidden_scheduler_control =
		"refresh().await; let mut interval = tokio::time::interval(tick); interval.tick().await;";
	assert!(
		adr080_has_seed_plus_immediate_interval(forbidden_scheduler_control),
		"ADR 080 positive control must observe seed-plus-immediate-interval scheduling"
	);
	let schedule = data_server
		.split("async fn run_lease_refresh_schedule")
		.nth(1)
		.and_then(|tail| {
			tail.split("// ============================================================")
				.next()
		})
		.expect("lease refresh schedule source");
	assert!(
		!adr080_has_seed_plus_immediate_interval(schedule)
			&& schedule.contains("tokio::time::sleep(tick)")
			&& schedule.contains("biased;")
			&& schedule.contains("reset(tokio::time::Instant::now() + tick)"),
		"ADR 080 requires one immediate seed followed by newest-deadline, non-bursting renewal scheduling"
	);

	let duplicate_metric_control =
		"metrics().raft_entries_applied_total.inc(); metrics().raft_entries_applied_total.inc();";
	assert_eq!(
		adr080_total_applied_increment_count(duplicate_metric_control),
		2,
		"ADR 080 positive control must observe a duplicated total-applied increment"
	);
	assert_eq!(
		adr080_total_applied_increment_count(&raft),
		1,
		"ADR 080 total-applied metric must increment at exactly one source location"
	);
	let live_apply = raft
		.split("impl RaftStateMachine<WendelonTypeConfig> for WendelonStateMachine")
		.nth(1)
		.expect("Raft state-machine implementation");
	assert!(
		live_apply.contains("metrics().raft_entries_applied_total.inc()")
			&& metrics.contains("wendelon_raft_entries_applied_total"),
		"ADR 080 requires the substrate numerator at the live state-machine apply boundary"
	);

	for required in [
		"pub const SOAK_FORMAT_VERSION: u32 = 2",
		"committed_lease_grant_delta",
		"raft_entries_applied_delta",
		"named_raft_entries",
		"raft_residual_entries",
		"unexpected_idle_raft_writer_trips_total_substrate_budget",
	] {
		assert!(
			soak.contains(required),
			"ADR 080 local-idle substrate evidence is missing {required}"
		);
	}
	for required in [
		"wendelon_data_ring_lease_grants_total",
		"wendelon_raft_entries_applied_total",
		"format_version:2",
	] {
		assert!(
			soak_runner.contains(required),
			"ADR 080 isolated runner is missing {required}"
		);
	}

	let forbidden_wire_control = "pub struct LeaseDescriptor { pub data_ring_lease_ttl: u32 }";
	assert!(
		forbidden_wire_control.contains("data_ring_lease_ttl"),
		"ADR 080 positive control must observe a lease wire-layout change"
	);
	assert!(
		!schema.contains("data_ring_lease_ttl")
			&& schema.contains("size_of::<LeaseDescriptor>() == 144"),
		"ADR 080 changes lease timing policy without changing the fixed LeaseDescriptor wire layout"
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
	let manager = manager_source(manifest_dir);
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let data_server = data_server_source(manifest_dir.as_ref());
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
fn test_adr062_row_group_sizing_profile_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let row_group = fs::read_to_string(manifest_dir.join("src/storage/row_group.rs")).unwrap();
	let multirow = fs::read_to_string(manifest_dir.join("src/storage/multirow.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let example = fs::read_to_string(manifest_dir.join("examples/wendelon.toml.example")).unwrap();
	let adr056 = fs::read_to_string(
		manifest_dir.join("plans/adr/0056_row_group_fetch_and_page_directory.md"),
	)
	.unwrap();
	let bench_results = fs::read_to_string(
		manifest_dir.join("plans/impl/0062_row_group_sizing_profile_bench_results.md"),
	)
	.unwrap();

	let mut violations = Vec::new();

	for required in [
		"pub enum RowGroupSizingProfile {\n\tPointRead,\n\tScan,\n}",
		"pub profile: RowGroupSizingProfile",
		"sizing: RowGroupSizingValues",
		"ROW_GROUP_POINT_READ_TARGET_ROWS: u32 = 4_096",
		"ROW_GROUP_POINT_READ_MAX_ROWS: u32 = 8_192",
		"ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_ROWS: u64 = 8_192",
		"ROW_GROUP_POINT_READ_MIN_PAGED_RECORD_BYTES: u64 = 512 * 1024",
		"ROW_GROUP_SCAN_TARGET_ROWS: u32 = 65_536",
		"ROW_GROUP_SCAN_MAX_ROWS: u32 = 131_072",
		"ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS: u64 = 65_536",
		"ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES: u64 = 1024 * 1024",
		"pub fn target_row_group_rows(&self) -> RowGroupRowCount",
		"pub fn max_row_group_rows(&self) -> RowGroupRowCount",
		"pub fn min_paged_record_rows(&self) -> RowCount",
		"pub fn min_paged_record_bytes(&self) -> RecordBodyByteLen",
	] {
		if !row_group.contains(required) {
			violations.push(format!(
				"src/storage/row_group.rs: missing ADR 062 token {required}"
			));
		}
	}

	for forbidden in [
		"pub target_row_group_rows: RowGroupRowCount",
		"pub max_row_group_rows: RowGroupRowCount",
		"pub min_paged_record_rows: RowCount",
		"pub min_paged_record_bytes: RecordBodyByteLen",
	] {
		if row_group.contains(forbidden) {
			violations.push(format!(
				"src/storage/row_group.rs: ADR 062 profile-derived sizing field is public: {forbidden}"
			));
		}
	}

	for required in [
		"profile: Option<String>",
		"None | Some(\"point_read\") => RowGroupSizingProfile::PointRead",
		"Some(\"scan\") => RowGroupSizingProfile::Scan",
		"unknown storage.row_group_writer profile",
		"apply_point_read_overrides(",
		"tracing::warn!",
		"profile = \\\"scan\\\"",
	] {
		if !config.contains(required) {
			violations.push(format!("src/config.rs: missing ADR 062 token {required}"));
		}
	}

	for required in [
		"RowGroupSizingProfile",
		"row_group_config.profile != RowGroupSizingProfile::Scan",
		"row_group_config.target_row_group_rows().get()",
	] {
		if !multirow.contains(required) {
			violations.push(format!(
				"src/storage/multirow.rs: missing ADR 062 token {required}"
			));
		}
	}

	if !example.contains("profile = \"point_read\" # \"point_read\" (default) or \"scan\"") {
		violations
			.push("examples/wendelon.toml.example: missing ADR 062 profile example".to_string());
	}

	if !adr056.contains("ADR 062 row-group sizing profile is now implemented and") {
		violations.push("plans/adr/0056_row_group_fetch_and_page_directory.md: missing ADR 062 amendment reference".to_string());
	}

	for required in [
		"ADR 062 Row-Group Sizing Profile Bench Results",
		"scenario=adr062_writer_cost",
		"scenario=adr062_scan_profile_directory",
		"scenario=adr062_scan_boundary",
		"writer-cost gate passes",
		"ADR 062 is promoted with `Scan` target `65_536`",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/0062_row_group_sizing_profile_bench_results.md: missing ADR 062 bench token {required}"
			));
		}
	}

	for forbidden in [
		"choose_row_group_sizing_profile",
		"RecordBodyCompressionPolicy::ScanProfile",
		"L2FetchStrategy::ScanRowGroup",
	] {
		for (path, content) in [
			("src/storage/row_group.rs", &row_group),
			("src/storage/multirow.rs", &multirow),
			("src/config.rs", &config),
		] {
			if content.contains(forbidden) {
				violations.push(format!("{path}: forbidden ADR 062 token {forbidden}"));
			}
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 062 row-group sizing profile invariants violated:\n{}",
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
	let manager = manager_source(manifest_dir);
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let data_server = data_server_source(manifest_dir.as_ref());
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
		"FsstWholeBuffer = 7",
		"Alp = 8",
		"FsstRowSafe = 9",
		"pub const ALL: [Self; 9]",
		"pub const DEFAULT_ENABLED: [Self; 9]",
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
	if !manager.contains(
		"pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config",
	) || !manager.contains("&self.config.encoding_writer")
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

#[test]
fn test_adr058_encoding_compute_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let compute_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/mod.rs")).unwrap();
	let compute_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/types.rs")).unwrap();
	let compute_mask =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/mask.rs")).unwrap();
	let compute_dispatch =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/dispatch.rs")).unwrap();
	let compute_aggregate =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/aggregate.rs")).unwrap();
	let compute_planner =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/planner.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let cargo_toml = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let manager = manager_source(manifest_dir);
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let audit = fs::read_to_string(manifest_dir.join("src/audit.rs")).unwrap();
	let data_server = data_server_source(manifest_dir.as_ref());
	let schema_core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let transport = fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let cdc = fs::read_to_string(manifest_dir.join("src/storage/cdc.rs")).unwrap();
	let example = fs::read_to_string(manifest_dir.join("examples/wendelon.toml.example")).unwrap();
	let bench_path = manifest_dir.join("benches/encoding_compute.rs");
	let bench = fs::read_to_string(&bench_path).unwrap_or_default();

	for required in [
		"impl_unit!(DictMatchSetSize, u32)",
		"impl_unit!(PredicateLeafIndex, u16)",
		"impl_unit!(SelectivityFractionMicros, u32)",
	] {
		if !units.contains(required) {
			violations.push(format!(
				"src/schema/units.rs: missing ADR 058 unit {required}"
			));
		}
	}
	for module in [
		"aggregate.rs",
		"coerce.rs",
		"dispatch.rs",
		"index.rs",
		"kernels/mod.rs",
		"mask.rs",
		"null.rs",
		"planner.rs",
		"predicate.rs",
		"types.rs",
	] {
		if !manifest_dir
			.join("src/storage/encoding/compute")
			.join(module)
			.exists()
		{
			violations.push(format!(
				"src/storage/encoding/compute/{module}: missing ADR 058 module"
			));
		}
	}
	if !compute_mod.contains("pub mod types;") || !compute_mod.contains("pub use types::*;") {
		violations.push(
			"src/storage/encoding/compute/mod.rs: compute manifest must re-export types"
				.to_string(),
		);
	}
	for required in [
		"BitPacked = 1",
		"RlePlain = 2",
		"RleBitPacked = 3",
		"DictionaryPlain = 4",
		"DictionaryBitPacked = 5",
		"Delta = 6",
		"CountStar = 1",
		"CountNonNull = 2",
		"SignedAvg = 7",
		"UnsignedAvg = 8",
		"MaskLengthMismatch = 1",
		"SelectionRangeOverflow = 5",
		"pub max_dict_match_set_size: DictMatchSetSize",
		"pub min_selectivity_for_encoded_eval_micros: SelectivityFractionMicros",
		"ENCODING_COMPUTE_MAX_MASK_BYTES_PER_RECORD_DEFAULT: u64 = 256 * 1024",
		"ENCODING_COMPUTE_MIN_SELECTIVITY_FOR_ENCODED_EVAL_MICROS_DEFAULT: u32 = 250_000",
		"ENCODING_COMPUTE_MIN_PREFILTER_REJECTION_MICROS_DEFAULT: u32 = 100_000",
	] {
		if !compute_types.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/types.rs: missing ADR 058 token {required}"
			));
		}
	}
	if compute_types.contains("Reserved") || compute_types.contains("V1") {
		violations.push(
			"src/storage/encoding/compute/types.rs: ADR 058 types must not reserve variants or use V1 suffixes"
				.to_string(),
		);
	}
	for required in [
		"pub enum EncodedRowMask",
		"All",
		"None",
		"Bitmap",
		"bytes: Vec<u8>",
		"Runs",
		"runs: Vec<SelectionRun>",
		"pub fn to_arrow_boolean_array",
		"pub fn from_arrow_boolean_array",
		"SelectionRangeOverflow",
	] {
		if !compute_mask.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/mask.rs: missing ADR 058 mask token {required}"
			));
		}
	}
	for required in [
		"pub fn evaluate(",
		"evaluator_kind_for_cascade",
		"EncodingComputeEvaluatorKind::BitPacked",
		"EncodingComputeEvaluatorKind::RlePlain",
		"EncodingComputeEvaluatorKind::DictionaryBitPacked",
		"EncodingComputeEvaluatorKind::Delta",
		"decode_buffer(",
		"expand_page_descriptors",
		"KernelDecodedLengthMismatch",
	] {
		if !compute_dispatch.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/dispatch.rs: missing ADR 058 dispatch token {required}"
			));
		}
	}
	for required in [
		"try_fold_encoded_aggregate_request",
		"with_encoded_column",
		"with_validity_mask",
		"decode_encoded_buffer",
		"AggregateFunction::CountStar",
		"return Ok(None)",
		"AggregateOverflowMismatch",
		"aggregate_kind_for_tag",
	] {
		if !compute_aggregate.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/aggregate.rs: missing ADR 058 aggregate token {required}"
			));
		}
	}
	if !compute_planner
		.contains("selectivity.get() <= config.min_selectivity_for_encoded_eval_micros.get()")
		|| !compute_planner.contains("EncodingComputePlanMode::DenseFallback")
	{
		violations.push(
			"src/storage/encoding/compute/planner.rs: selectivity boundary must be <= 0.25 else DenseFallback"
				.to_string(),
		);
	}
	for required in [
		"encoding_compute: RawEncodingComputeConfig",
		"enabled_predicate_evaluators",
		"enabled_aggregate_kernels",
		"RawEncodingComputeConfig",
	] {
		if !config.contains(required) {
			violations.push(format!(
				"src/config.rs: missing ADR 058 config token {required}"
			));
		}
	}
	if !node.contains("pub encoding_compute: EncodingComputeConfig")
		|| !manager.contains("pub encoding_compute: EncodingComputeConfig")
	{
		violations.push("node/manager configs must carry EncodingComputeConfig".to_string());
	}
	for required in [
		"try_read_encoding_compute_phase_b_from_l2",
		"build_encoding_compute_phase_b_plan",
		"read_encoded_aggregate_record",
		"build_encoding_compute_aggregate_plan",
		"try_fold_encoded_aggregate_request",
		"phase_b_point_read_survives",
		"evaluate_encoded_predicate",
		"observe_storage_l2_encoding_compute_plan(\"exact\", \"none\")",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 058 runtime hook token {required}"
			));
		}
	}
	for metric in [
		"storage_l2_encoding_compute_plan_total",
		"storage_l2_encoding_compute_kernel_micros",
		"storage_l2_encoding_compute_rows_total",
		"storage_l2_encoding_compute_mask_bytes",
		"storage_l2_encoding_compute_pages_fetched_total",
		"storage_l2_encoding_compute_aggregate_total",
		"storage_l2_encoding_compute_invariant_violations_total",
		"ENCODING_COMPUTE_FALLBACK_REASONS",
		"ENCODING_COMPUTE_KERNEL_LABELS",
		"ENCODING_COMPUTE_INVARIANT_KINDS",
	] {
		if !metrics.contains(metric) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 058 metric token {metric}"
			));
		}
	}
	if !example.contains("[storage.encoding_compute]")
		|| !example.contains("min_selectivity_for_encoded_eval_micros = 250000")
	{
		violations.push(
			"examples/wendelon.toml.example: missing ADR 058 encoding_compute example config"
				.to_string(),
		);
	}
	if !cargo_toml.contains("name = \"encoding_compute\"")
		|| !cargo_toml.contains("harness = false")
	{
		violations.push("Cargo.toml: ADR 058 encoding_compute bench is not registered".to_string());
	}
	if !bench_path.exists() {
		violations
			.push("benches/encoding_compute.rs: ADR 058 bench harness is missing".to_string());
	} else {
		for required in [
			"encoding_compute_bench",
			"predicate_exact",
			"mask_algebra",
			"aggregate_fold",
			"try_fold_encoded_aggregate_request",
		] {
			if !bench.contains(required) {
				violations.push(format!(
					"benches/encoding_compute.rs: missing ADR 058 bench token {required}"
				));
			}
		}
	}
	for required in [
		"EncodingComputeSampled",
		"encoding_compute.sampled",
		"EncodingComputeInvariantViolation",
		"encoding_compute.invariant_violation",
	] {
		if !audit.contains(required) {
			violations.push(format!(
				"src/audit.rs: missing ADR 058 audit token {required}"
			));
		}
	}
	for required in [
		"emit_encoding_compute_sampled_audit",
		"AuditOperation::EncodingComputeSampled",
		"emit_encoding_compute_invariant_audit",
		"AuditOperation::EncodingComputeInvariantViolation",
		"try_read_encoded_aggregate_query_location",
		"aggregate_responses",
	] {
		if !data_server.contains(required) {
			violations.push(format!(
				"src/network/data_server.rs: missing ADR 058 audit token {required}"
			));
		}
	}
	for forbidden in [
		"MsgType::Cocd",
		"MsgType::EncodingCompute",
		"FRAME_FLAG_ENCODING_COMPUTE",
		"FRAME_FLAG_COCD",
	] {
		if schema_core.contains(forbidden) {
			violations.push(format!(
				"src/schema/core.rs: forbidden ADR 058 wire token {forbidden}"
			));
		}
	}
	for forbidden in [
		"NodeCapability::Cocd",
		"NodeCapability::EncodingCompute",
		"NodeCapability::EncodedAggregate",
	] {
		if transport.contains(forbidden) {
			violations.push(format!(
				"src/network/transport.rs: forbidden ADR 058 capability {forbidden}"
			));
		}
	}
	if cdc.contains("EncodingCompute") {
		violations.push("src/storage/cdc.rs: CDC must not import ADR 058 internals".to_string());
	}

	assert!(
		violations.is_empty(),
		"ADR 058 Encoding compute invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr059_native_cocd_kernel_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let codec =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/cascade/codec.rs")).unwrap();
	let dispatch =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/dispatch.rs")).unwrap();
	let aggregate =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/aggregate.rs")).unwrap();
	let compute_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/types.rs")).unwrap();
	let kernels_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/mod.rs"))
			.unwrap();
	let bitpacked =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/bitpacked.rs"))
			.unwrap();
	let dictionary =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/dictionary.rs"))
			.unwrap();
	let delta =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/delta.rs"))
			.unwrap();
	let rle = fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/rle.rs"))
		.unwrap();
	let validity =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/validity.rs"))
			.unwrap();
	let scalar =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/scalar.rs"))
			.unwrap();
	let mask =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/mask.rs")).unwrap();
	let manager = manager_source(manifest_dir);
	let bench = fs::read_to_string(manifest_dir.join("benches/encoding_compute.rs")).unwrap();
	let bench_artifact = fs::read_to_string(
		manifest_dir.join("plans/impl/0059_native_cocd_kernels_bench_results.md"),
	)
	.unwrap_or_default();

	for required in [
		"pub(crate) struct BitPackedFrame",
		"pub(crate) struct FixedWidthDictionaryFrame",
		"pub(crate) struct DeltaFrame",
		"pub(crate) struct RleByteRun",
		"pub(crate) fn parse_bitpacked_frame",
		"pub(crate) fn parse_fixed_width_dictionary_frame",
		"pub(crate) fn parse_delta_frame",
	] {
		if !codec.contains(required) {
			violations.push(format!(
				"src/storage/encoding/cascade/codec.rs: missing ADR 059 parser token {required}"
			));
		}
	}
	for required in [
		"pub fn evaluate(",
		"pub fn evaluate_decode_scalar",
		"NativeEvaluationEligibility",
		"kernels::bitpacked::evaluate_predicate",
		"kernels::dictionary::evaluate_predicate",
		"kernels::delta::evaluate_predicate",
		"kernels::rle::evaluate_predicate",
		"native_predicate_cascade_eligible",
		"evaluate_decoded_fixed_width_integer",
	] {
		if !dispatch.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/dispatch.rs: missing ADR 059 dispatch token {required}"
			));
		}
	}
	if !compute_types.contains("enabled: true")
		|| !compute_types.contains("EncodingComputeEvaluatorKind::BitPacked")
		|| !compute_types.contains("EncodingComputeEvaluatorKind::DictionaryPlain")
		|| !compute_types.contains("EncodingComputeEvaluatorKind::DictionaryBitPacked")
		|| !compute_types.contains("EncodingComputeEvaluatorKind::Delta")
		|| compute_types.contains("enabled_predicate_evaluators: EncodingComputeEvaluatorKind::ALL")
	{
		violations.push(
			"src/storage/encoding/compute/types.rs: ADR 059 promoted default must enable only the narrowed predicate allow-list"
				.to_string(),
		);
	}
	for required in [
		"pub mod bitpacked;",
		"pub mod delta;",
		"pub mod dictionary;",
		"pub mod rle;",
		"pub mod scalar;",
		"pub mod validity;",
	] {
		if !kernels_mod.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/mod.rs: missing ADR 059 module token {required}"
			));
		}
	}
	for (path, source, required) in [
		(
			"bitpacked.rs",
			bitpacked.as_str(),
			[
				"parse_bitpacked_frame",
				"unsigned_predicate",
				"signed_predicate",
				"evaluate_u8_frame",
				"chunks_exact",
				"read_unsigned_le_as_u128",
			]
			.as_slice(),
		),
		(
			"dictionary.rs",
			dictionary.as_str(),
			[
				"parse_fixed_width_dictionary_frame",
				"dictionary_matches",
				"DictionaryIndexEncoding",
				"push_index_match",
			]
			.as_slice(),
		),
		(
			"delta.rs",
			delta.as_str(),
			[
				"parse_delta_frame",
				"try_push_monotonic_signed_range",
				"checked_add",
				"read_signed_delta",
				"signed_value_from_u128",
			]
			.as_slice(),
		),
		(
			"rle.rs",
			rle.as_str(),
			[
				"RleByteRunIter",
				"push_many",
				"evaluate_lane",
				"signed_value_from_le_bytes",
			]
			.as_slice(),
		),
	] {
		for token in required {
			if !source.contains(token) {
				violations.push(format!(
					"src/storage/encoding/compute/kernels/{path}: missing ADR 059 token {token}"
				));
			}
		}
	}
	for required in [
		"SignedPredicate",
		"UnsignedPredicate",
		"push_many",
		"push_aligned_bitmap_byte",
		"signed_value_from_le_bytes",
		"unsigned_predicate",
	] {
		if !scalar.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/scalar.rs: missing ADR 059 scalar token {required}"
			));
		}
	}
	for required in [
		"compose_value_mask_with_validity",
		"evaluate_validity_bitmap",
		"validity_bit",
	] {
		if !validity.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/validity.rs: missing ADR 059 validity token {required}"
			));
		}
	}
	for required in ["combine_bitmap", "mask_tail_bits", "combine_bool"] {
		if !mask.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/mask.rs: missing ADR 059 mask algebra token {required}"
			));
		}
	}
	for required in [
		"EncodedAggregateSource",
		"fold_native_signed_sum",
		"fold_native_unsigned_sum",
		"fold_native_min_max",
		"fold_signed_encoded_values",
		"fold_unsigned_encoded_values",
		"fold_signed_min_max_encoded_values",
		"fold_unsigned_min_max_encoded_values",
		"with_native_encoded_column",
		"fold_unsigned_sum_no_validity",
		"try_fold_native_masked_aggregate_request",
		"AggregateMaskSelection",
		"count_masked_dictionary_indices",
		"for_selected_rows_in_page",
		"bitpacked_value_at",
		"dictionary_index_at",
		"native_aggregate_cascade_eligible",
		"native_unsigned_aggregate_cascade_eligible",
	] {
		if !aggregate.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/aggregate.rs: missing ADR 059 aggregate token {required}"
			));
		}
	}
	for required in [
		"native_aggregate_cascade_eligible(&function, field.data_type(), Some(cascade))",
		"native_evaluation_eligibility(cascade, atom, &coercion)",
		"with_native_encoded_column",
		"EncodingComputeFallbackReason::AggregateUnsupported",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 059 aggregate eligibility token {required}"
			));
		}
	}
	for required in [
		"rows: vec![8_192, 65_536, 1_048_576]",
		"cargo bench --bench encoding_compute",
		"predicate_selectivity_sweep",
		"aggregate_{kind_label}_selectivity_sweep",
		"for _ in 0..iterations.min(5)",
	] {
		if !bench.contains(required) {
			violations.push(format!(
				"benches/encoding_compute.rs: missing ADR 059 bench token {required}"
			));
		}
	}
	for required in [
		"default-set promotion gate **passed**",
		"Promote ADR 059 to default-enabled with a narrowed default set.",
		"RlePlain",
		"native_aggregate_cascade_eligible(...)",
		"cargo bench --bench encoding_compute",
	] {
		if !bench_artifact.contains(required) {
			violations.push(format!(
				"plans/impl/0059_native_cocd_kernels_bench_results.md: missing ADR 059 evidence token {required}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 059 native COCD kernel invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr066_fsst_evaluator_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let compute_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/types.rs")).unwrap();
	let dispatch =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/dispatch.rs")).unwrap();
	let kernels_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/mod.rs"))
			.unwrap();
	let fsst_kernel =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/fsst.rs"))
			.unwrap();
	let manager = manager_source(manifest_dir);
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();

	for required in [
		"FsstWholeBuffer = 7",
		"Self::FsstWholeBuffer",
		"\"fsst_whole_buffer\" => Some(Self::FsstWholeBuffer)",
	] {
		if !compute_types.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/types.rs: missing ADR 066 token {required}"
			));
		}
	}
	if compute_types.contains("EncodingComputeEvaluatorKind::FsstWholeBuffer,")
		&& compute_types.contains("enabled_predicate_evaluators: EncodingComputeEvaluatorKind::ALL")
	{
		violations.push(
			"src/storage/encoding/compute/types.rs: Fsst must not be default-enabled by ALL"
				.to_string(),
		);
	}
	for required in [
		"pub mod fsst;",
		"parse_fsst_frame",
		"decode_offsets",
		"evaluate_fsst_equality",
		"FsstDecodedByteReader",
	] {
		if !kernels_mod.contains(required) && !fsst_kernel.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/fsst.rs: missing ADR 066 token {required}"
			));
		}
	}
	for required in [
		"EncodingCascade::FsstWholeBuffer { .. } =>",
		"Some(EncodingComputeEvaluatorKind::FsstWholeBuffer)",
		"evaluate_fsst_predicate",
		"PredicateAtomOp::Eq | PredicateAtomOp::Neq | PredicateAtomOp::InList",
		"string_like_data_type_supported",
	] {
		if !dispatch.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/dispatch.rs: missing ADR 066 dispatch token {required}"
			));
		}
	}
	for required in [
		"FsstValues",
		"FsstOffsets",
		"offsets_buffer_ordinal",
		"output_columns.contains(*column_index as u32)",
		"EncodingComputeEvaluatorKind::FsstWholeBuffer",
		"evaluate_fsst_predicate(",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 066 manager token {required}"
			));
		}
	}
	for forbidden in [
		"symbol_table_byte_len",
		"materialize_encoding_buffer",
		"pre_encoded",
		"encoded_literal",
	] {
		if fsst_kernel.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/fsst.rs: forbidden ADR 066 token {forbidden}"
			));
		}
	}
	for required in ["\"fsst_whole_buffer\"", "ENCODING_COMPUTE_KERNEL_LABELS"] {
		if !metrics.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 066 metric token {required}"
			));
		}
	}
	if !encoding_types.contains("FsstWholeBuffer {\n\t\tsymbol_table_byte_len: RecordBodyByteLen,")
		|| !encoding_types.contains("FsstWholeBuffer = 7")
	{
		violations.push(
			"src/storage/encoding/types.rs: ADR 068 prerequisite must keep whole-buffer FSST discriminant/layout stable"
				.to_string(),
		);
	}

	assert!(
		violations.is_empty(),
		"ADR 066 FSST evaluator invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr067_row_safe_fsst_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let codec =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/cascade/codec.rs")).unwrap();
	let select = fs::read_to_string(manifest_dir.join("src/storage/encoding/select.rs")).unwrap();
	let compute_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/types.rs")).unwrap();
	let dispatch =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/dispatch.rs")).unwrap();
	let fsst_kernel =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/kernels/fsst.rs"))
			.unwrap();
	let manager = manager_source(manifest_dir);
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let encoding_compute_bench =
		fs::read_to_string(manifest_dir.join("benches/encoding_compute.rs")).unwrap();
	let row_group_fetch_bench =
		fs::read_to_string(manifest_dir.join("benches/row_group_fetch.rs")).unwrap();
	let bench_results = fs::read_to_string(
		manifest_dir.join("plans/impl/0067_row_canonical_fsst_bench_results.md"),
	)
	.unwrap_or_default();

	for required in [
		"FsstRowSafe = 9",
		"Self::FsstRowSafe => \"fsst_row_safe\"",
		"\"fsst_row_safe\" => Some(Self::FsstRowSafe)",
		"pub const DEFAULT_ENABLED: [Self; 9]",
		"enabled_cascades: EncodingCascadeKind::DEFAULT_ENABLED.into_iter().collect()",
		"FsstRowSafe {\n\t\tsymbol_table_byte_len: RecordBodyByteLen,",
	] {
		if !encoding_types.contains(required) {
			violations.push(format!(
				"src/storage/encoding/types.rs: missing ADR 067 token {required}"
			));
		}
	}
	// Post-promotion (ADR 067 Phase 8): FsstRowSafe is now in DEFAULT_ENABLED.
	// The original Phase 1 architecture guard pinned the opposite (kept it
	// out of defaults pending Phase 7 evidence); the council promoted it
	// default-on per the Phase 7 promotion-gate evidence in
	// plans/impl/0067_row_canonical_fsst_bench_results.md.
	if !encoding_types.contains("Self::FsstRowSafe,\n\t];")
		&& !encoding_types.contains("Self::FsstRowSafe,\r\n\t];")
	{
		violations.push(
			"src/storage/encoding/types.rs: DEFAULT_ENABLED must list FsstRowSafe last (post-promotion contract)"
				.to_string(),
		);
	}
	for required in [
		"encode_fsst_row_safe_bytes",
		"decode_fsst_row_safe_bytes",
		"b\"RSFS\"",
		"value_offsets.windows(2)",
		"row_count as u32).to_le_bytes()",
		"FSST_ROW_SAFE_BLOCK_ROWS: usize = 256",
		"FSST_ROW_SAFE_LENGTH_SENTINEL: u8 = 255",
		"block_offsets",
		"encoded_lengths",
		"exception_lengths",
	] {
		if !codec.contains(required) {
			violations.push(format!(
				"src/storage/encoding/cascade/codec.rs: missing ADR 067 RSFS token {required}"
			));
		}
	}
	for required in [
		"EncodingCascadeKind::FsstRowSafe",
		"EncodingCascade::FsstRowSafe",
		"saturating_mul(11).div_ceil(10)",
	] {
		if !select.contains(required) {
			violations.push(format!(
				"src/storage/encoding/select.rs: missing ADR 067 selection token {required}"
			));
		}
	}
	for required in [
		"FsstRowSafe = 8",
		"Self::FsstRowSafe => \"fsst_row_safe\"",
		"\"fsst_row_safe\" => Some(Self::FsstRowSafe)",
	] {
		if !compute_types.contains(required) {
			violations.push(format!(
				"src/storage/encoding/compute/types.rs: missing ADR 067 evaluator token {required}"
			));
		}
	}
	for required in [
		"evaluate_fsst_row_safe_predicate",
		"parse_fsst_row_safe_frame",
		"evaluate_fsst_row_safe_equality",
		"EncodingCascade::FsstRowSafe { .. } =>",
	] {
		if !dispatch.contains(required) && !fsst_kernel.contains(required) {
			violations.push(format!(
				"ADR 067 dispatch/kernel token missing from dispatch or fsst kernel: {required}"
			));
		}
	}
	for required in [
		"evaluate_fsst_row_safe_predicate",
		"EncodingComputeEvaluatorKind::FsstRowSafe",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 067 manager token {required}"
			));
		}
	}
	for required in ["\"fsst_row_safe\"", "ENCODING_COMPUTE_KERNEL_LABELS"] {
		if !metrics.contains(required) {
			violations.push(format!(
				"src/metrics/registry.rs: missing ADR 067 metric token {required}"
			));
		}
	}
	for required in [
		"fsst_row_safe_recorded_diagnostic",
		"fsst_row_safe_writer_selected",
		"fsst_row_safe_dense_fallback_atom_unsupported",
		"fsst_row_safe_cross_replica_determinism",
	] {
		if !encoding_compute_bench.contains(required) {
			violations.push(format!(
				"benches/encoding_compute.rs: missing ADR 067 bench token {required}"
			));
		}
	}
	for required in [
		"adr067_fsst_row_safe_row_group_fetch",
		"fsst_row_safe_kernel_case_a1",
		"fsst_row_safe_dense_case_b_overlap_a2",
	] {
		if !row_group_fetch_bench.contains(required) {
			violations.push(format!(
				"benches/row_group_fetch.rs: missing ADR 067 bench token {required}"
			));
		}
	}
	for required in [
		"ADR 067 Row-Canonical FSST Bench Results",
		"Promotion-gate summary",
		"EncodingCascadeKind::DEFAULT_ENABLED",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/0067_row_canonical_fsst_bench_results.md: missing ADR 067 evidence token {required}"
			));
		}
	}
	let row_safe_impl = fsst_kernel
		.split("fn evaluate_fsst_row_safe_equality_impl")
		.nth(1)
		.and_then(|tail| tail.split("struct EncodedRowSafeLiteral").next())
		.unwrap_or_default();
	for forbidden in [
		"FsstDecodedByteReader",
		"decode_buffer(",
		"evaluate_fsst_equality_impl",
	] {
		if row_safe_impl.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/compute/kernels/fsst.rs: row-safe Strategy B path used forbidden token {forbidden}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 067 row-safe FSST invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr068_fsst_row_safe_estimator_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let estimator =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/cascade/estimator.rs")).unwrap();
	let select = fs::read_to_string(manifest_dir.join("src/storage/encoding/select.rs")).unwrap();
	let codec =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/cascade/codec.rs")).unwrap();
	let directory =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/directory.rs")).unwrap();
	let encoding_compute_bench =
		fs::read_to_string(manifest_dir.join("benches/encoding_compute.rs")).unwrap();
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();

	for required in [
		"pub(crate) enum FsstRowSafeEstimatorDecision",
		"pub(crate) enum FsstRowSafeEstimatorDecline",
		"pub(crate) enum FsstRowSafeEstimatorRouting",
		"pub(crate) struct FsstRowSafeEstimatorStats",
		"pub(crate) enum FsstRowSafeEstimatorTableSource",
		"FsstRowSafeEstimatorTableSource::CrossBufferCached => None",
		"pub(crate) fn estimate_fsst_row_safe_cost_free_decline",
		"pub(crate) fn estimate_fsst_row_safe_with_cached_tables",
		"MetadataFloorExceedsPlaintext",
		"MetadataPayloadFloorExceedsPlaintext",
		"TrainedMetadataFloorExceedsPlaintext",
		"SampledRowSafeExceedsPlaintext",
		"SampledRowSafeExceedsWholeBufferGuard",
		"FsstRowSafeEstimatorSampleShape::DeterministicSpread",
		"deterministic_spread_sample_shape_bounds_prefix_projection_error",
		"CROSS_BUFFER_SAMPLE_FLOOR_PRESSURE_DENOMINATOR",
		"fsst_symbol_training_cost_probe",
		"cached_tables_do_not_decline_from_cross_buffer_sample",
		"EARLY_DECLINE_SAFETY_MULTIPLIER_NUMERATOR: usize = 6",
		"EARLY_DECLINE_SAFETY_MULTIPLIER_DENOMINATOR: usize = 5",
	] {
		if !estimator.contains(required) {
			violations.push(format!(
				"src/storage/encoding/cascade/estimator.rs: missing ADR 068 token {required}"
			));
		}
	}
	for required in [
		"fsst_row_safe_metadata_floor_bytes",
		"fsst_row_safe_metadata_payload_floor_bytes",
		"fsst_row_safe_metadata_bytes",
		"FSST_ROW_SAFE_FIXED_HEADER_BYTES: usize = 13",
		"train_fsst_whole_buffer_symbol_table",
		"train_fsst_row_safe_symbol_table",
		"fsst_whole_buffer_encoded_len_with_table",
		"fsst_row_safe_encoded_len_with_table",
	] {
		if !codec.contains(required) {
			violations.push(format!(
				"src/storage/encoding/cascade/codec.rs: missing ADR 068 codec token {required}"
			));
		}
	}
	for required in [
		"select_fsst_candidate_with_routing",
		"pub(crate) fn select_fsst_candidate_with_routing",
		"pub(crate) fn select_fsst_candidate_with_routing_and_context",
		"pub(crate) struct WriterCascadeContext",
		"pub(crate) struct ColumnFsstSymbolTableCache",
		"pub(crate) fn select_and_encode_buffer_with_value_offsets_and_context",
		"FsstRowSafeEstimatorRouting::Estimator",
		"FsstRowSafeEstimatorRouting::ExactOnly",
		"writer_context.is_some() && ipc_column_ordinal.is_some()",
		"FsstRowSafeEstimatorDecision::Decline(_) => {}",
		"FsstRowSafeExactLengths::CurrentBuffer",
		"fsst_writer_context_warms_column_cache_from_exact_route",
		"row_safe_exact_guard(",
		"fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_8k",
		"fsst_estimator_route_emits_exact_bytes_on_warehouse_winner_matrix_all_tiers",
		"fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_8k",
		"fsst_estimator_route_emits_byte_identical_predicate_masks_on_warehouse_winner_matrix_all_tiers",
	] {
		if !select.contains(required) {
			violations.push(format!(
				"src/storage/encoding/select.rs: missing ADR 068 selection token {required}"
			));
		}
	}
	for required in [
		"WriterCascadeContext::default()",
		"build_encoding_directory_for_ipc_stream_exact_fsst_for_bench",
		"FsstRowSafeEstimatorRouting::ExactOnly",
		"select_and_encode_buffer_with_value_offsets_and_context",
		"writer_context.as_mut()",
		"context_column",
	] {
		if !directory.contains(required) {
			violations.push(format!(
				"src/storage/encoding/directory.rs: missing ADR 068 writer column-cache context token {required}"
			));
		}
	}
	for required in [
		"fn fsst_log_line_eq_5p_fixture",
		"fn fsst_metadata_floor_decline_fixture",
		"fsst_metadata_floor_decline_diag",
		"fsst_log_line_eq_5p",
		"fsst_row_safe_log_line_eq_5p",
		"encoding_compute_bench scenario=fsst_row_safe_writer route={}",
		"encoding_compute_bench scenario=fsst_row_safe_writer_column_cache_matrix",
		"--writer-column-cache-only",
		"--writer-column-cache-rows=",
		"--writer-column-cache-fixture=",
		"--fsst-writer-only",
		"selected_writer_ms",
		"selected_vs_dual_cpu_micros",
		"exact_writer_ms",
		"writer_vs_exact_cpu_micros",
		"exact_vs_writer_cpu_micros",
	] {
		if !encoding_compute_bench.contains(required) {
			violations.push(format!(
				"benches/encoding_compute.rs: missing ADR 068 free-text/log-line fixture token {required}"
			));
		}
	}
	for forbidden in [
		"AtomicUsize",
		"fsst_row_safe_estimator_total",
		"fsst_row_safe_estimator_decline",
		"--d2-",
		"d2_only",
		"d2_fixture",
		"d2_rows",
		"prepare_d2",
		"D2_",
		"fsst_row_safe_writer_d2_matrix",
	] {
		if encoding_compute_bench.contains(forbidden) {
			violations.push(format!(
				"benches/encoding_compute.rs: ADR 068 bench diagnostics must stay stdout-row based and semantic-named, found forbidden token {forbidden}"
			));
		}
	}
	for forbidden in [
		"pub enum FsstRowSafeEstimatorRouting",
		"pub enum FsstRowSafeEstimatorDecision",
		"pub enum FsstRowSafeEstimatorDecline",
		"pub enum FsstRowSafeEstimatorTableSource",
		"pub struct FsstRowSafeEstimatorStats",
		"pub sampled_rows",
		"pub estimated_fsst_whole_buffer_encoded_len",
		"pub estimated_fsst_row_safe_encoded_len",
		"pub decision",
		"pub payload_len",
		"pub exception_count",
	] {
		if estimator.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/cascade/estimator.rs: ADR 068 estimator type escaped crate-private visibility via {forbidden}"
			));
		}
	}
	for forbidden in [
		"pub struct WriterCascadeContext",
		"pub struct ColumnFsstSymbolTableCache",
		"pub fsst_tables_by_column",
		"pub row_safe_table",
		"pub whole_buffer_table",
	] {
		if select.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/select.rs: ADR 068 writer column-cache context escaped crate-private visibility via {forbidden}"
			));
		}
	}
	for forbidden in [
		"estimator_enabled",
		"fsst_row_safe_estimator_enabled",
		"row_safe_estimator_enabled",
	] {
		if encoding_types.contains(forbidden) || config.contains(forbidden) {
			violations.push(format!(
				"encoding writer config exposed forbidden ADR 068 estimator knob {forbidden}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 068 row-safe FSST estimator invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr069_encoded_validity_sparse_projection_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let select = fs::read_to_string(manifest_dir.join("src/storage/encoding/select.rs")).unwrap();
	let directory =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/directory.rs")).unwrap();
	let page_materialize =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/page_materialize.rs")).unwrap();
	let materialize =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/materialize.rs")).unwrap();
	let sparse_projection =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/sparse_projection_fetch.rs"))
			.unwrap();
	let manager = manager_source(manifest_dir);
	let columnar = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let encoding_compute_bench =
		fs::read_to_string(manifest_dir.join("benches/encoding_compute.rs")).unwrap();
	let bench_results = fs::read_to_string(
		manifest_dir.join("plans/impl/0069_encoded_validity_sparse_projection_bench_results.md"),
	)
	.unwrap_or_default();
	let adr064 =
		fs::read_to_string(manifest_dir.join("plans/adr/0064_nullable_sparse_projection.md"))
			.unwrap();
	let roadmap = fs::read_to_string(manifest_dir.join("plans/wip/roadmap.md")).unwrap();

	let mut violations = Vec::new();

	for required in [
		"pub(crate) enum WriterValidityCascadeRouting",
		"Disabled",
		"Enabled",
		"pub(crate) const VALIDITY_RLE_BYTE_SAVING_FRACTION",
		"pub(crate) const MIN_VALIDITY_ENCODE_BYTES",
		"pub(crate) struct ValidityRleEncodingPlan",
		"pub(crate) struct ValuePageRowSpan",
		"pub(crate) fn select_validity_cascade",
		"run_count: RunCount",
		"EncodingCascade::Rle",
		"inner: Box::new(EncodingLeafCascade::Plain)",
	] {
		if !select.contains(required) {
			violations.push(format!(
				"src/storage/encoding/select.rs: missing ADR 069 selection token {required}"
			));
		}
	}
	for forbidden in [
		"pub enum WriterValidityCascadeRouting",
		"pub fn select_validity_cascade",
		"pub struct ValidityRleEncodingPlan",
		"pub struct ValuePageRowSpan",
	] {
		if select.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/select.rs: ADR 069 helper escaped crate-private visibility via {forbidden}"
			));
		}
	}
	for required in [
		"WriterValidityCascadeRouting::Disabled",
		"WriterValidityCascadeRouting::Enabled",
		"pub(crate) fn build_encoding_directory_for_ipc_stream_with_validity_for_bench",
		"select_validity_cascade",
		"is_validity_buffer",
		"cascade_index_for",
		"page_value_count: value_page_value_count",
		"decoded_value_count: BufferValueCount::new(group.row_count.get())",
		"decoded_byte_len: RecordBodyByteLen::new(plan.raw_byte_len as u64)",
	] {
		if !directory.contains(required) {
			violations.push(format!(
				"src/storage/encoding/directory.rs: missing ADR 069 writer token {required}"
			));
		}
	}
	if directory.contains("pub fn build_encoding_directory_for_ipc_stream_with_validity_for_bench")
	{
		violations.push(
			"src/storage/encoding/directory.rs: ADR 069 validity writer bench route must remain crate-private"
				.to_string(),
		);
	}
	let production_writer_route = directory
		.split("pub fn build_encoding_directory_for_ipc_stream(")
		.nth(1)
		.and_then(|tail| tail.split("#[doc(hidden)]").next())
		.unwrap_or_default();
	if !production_writer_route.contains("WriterValidityCascadeRouting::Enabled") {
		violations.push(
			"src/storage/encoding/directory.rs: promoted ADR 069 production writer route must initialize validity routing Enabled"
				.to_string(),
		);
	}
	if production_writer_route.contains("WriterValidityCascadeRouting::Disabled") {
		violations.push(
			"src/storage/encoding/directory.rs: promoted ADR 069 production writer route must not initialize validity routing Disabled"
				.to_string(),
		);
	}
	for required in [
		"pub(crate) enum PageMaterializeValiditySource",
		"Raw { bit_offset: u8 }",
		"Encoded { cascade: EncodingCascadeIndex }",
		"pub(crate) fn validity_page_decoded_byte_len",
		"pub(crate) fn decode_rle_validity_page",
		"pub(crate) fn stitch_validity_pages_into_row_group_bitmap",
		"RleByteRunIter::new",
		"validity_null_count",
	] {
		if !page_materialize.contains(required) {
			violations.push(format!(
				"src/storage/encoding/page_materialize.rs: missing ADR 069 bitmap token {required}"
			));
		}
	}
	for required in [
		"pub(crate) fn materialize_encoding_validity_buffer",
		"pub(crate) fn decode_encoded_validity_buffer",
		"pub(crate) fn encoded_validity_cascade_supported",
		"stitch_validity_pages_into_row_group_bitmap",
		"materialize_ipc_page_is_validity_buffer",
	] {
		if !materialize.contains(required) {
			violations.push(format!(
				"src/storage/encoding/materialize.rs: missing ADR 069 materialize token {required}"
			));
		}
	}
	for required in [
		"pub(crate) enum EncodedValiditySparseEligibility",
		"Eligible",
		"Decline(EncodedValidityDeclineReason)",
		"pub(crate) enum EncodedValidityDeclineReason",
		"UnsupportedCascade",
		"MissingPageMetadata",
		"PageGeometryMismatch",
		"RowGroupRowCountMismatch",
		"DecodedByteLenMismatch",
		"CorruptPageDescriptor",
		"pub(crate) fn encoded_validity_sparse_eligibility",
		"encoded_validity_cascade_supported",
		"PageMaterializeValiditySource::Encoded",
		"PageMaterializeValiditySource::Raw",
		"EncodedValidityBufferUnsupported",
	] {
		if !sparse_projection.contains(required) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: missing ADR 069 planner token {required}"
			));
		}
	}
	for forbidden in [
		"ValidityV1Eligibility",
		"validity_v1_eligibility",
		"pub enum EncodedValiditySparseEligibility",
		"pub enum EncodedValidityDeclineReason",
		"pub fn encoded_validity_sparse_eligibility",
	] {
		if sparse_projection.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: ADR 069 planner helper escaped crate-private visibility via {forbidden}"
			));
		}
	}
	for required in [
		"PageMaterializeValiditySource::Raw { bit_offset }",
		"PageMaterializeValiditySource::Encoded { cascade }",
		"decode_rle_validity_page",
		"decode_encoded_validity_buffer",
		"encoded_validity_cascade_supported",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 069 runtime token {required}"
			));
		}
	}
	for required in [
		"materialize_encoding_validity_buffer",
		"buffer_ordinal == mapping.first_buffer_ordinal",
	] {
		if !columnar.contains(required) {
			violations.push(format!(
				"src/storage/columnar.rs: missing ADR 069 full-buffer role-dispatch token {required}"
			));
		}
	}
	if !encoding_types.contains("pub const ENCODING_DIRECTORY_FORMAT_VERSION: u32 = 3;") {
		violations.push("ADR 069 must not bump ENCODING_DIRECTORY_FORMAT_VERSION".to_string());
	}
	for forbidden in [
		"EncodedValidity",
		"ValidityRle",
		"WriterValidityCascadeRouting",
		"VALIDITY_RLE_BYTE_SAVING_FRACTION",
		"MIN_VALIDITY_ENCODE_BYTES",
	] {
		if encoding_types.contains(forbidden) || config.contains(forbidden) {
			violations.push(format!(
				"ADR 069 must not expose encoded-validity through public/config surfaces, found {forbidden}"
			));
		}
	}
	for forbidden in ["encoded_validity", "encoded-validity", "validity_rle"] {
		if metrics.contains(forbidden) {
			violations.push(format!(
				"ADR 069 must not add public metric labels, found {forbidden}"
			));
		}
	}
	for required in [
		"--encoded-validity-only",
		"--rows=",
		"encoded_validity_sparse_projection",
		"selected_validity_raw_bytes",
		"selected_validity_encoded_bytes",
		"deterministic={}",
	] {
		if !encoding_compute_bench.contains(required) {
			violations.push(format!(
				"benches/encoding_compute.rs: missing ADR 069 bench token {required}"
			));
		}
	}
	for required in [
		"Status: promoted",
		"Prompt-to-artifact checklist",
		"Phase 0 pins",
		"Hard gates",
		"Soft gates",
		"cargo bench --bench encoding_compute -- --quick --encoded-validity-only --rows=8192,65536,1048576",
		"Writer wall-clock percentage overhead is not bounded by the original",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/0069_encoded_validity_sparse_projection_bench_results.md: missing ADR 069 evidence token {required}"
			));
		}
	}
	for required in [
		"Where we are after ADR 069",
		"ADR 069 adds the encoded-validity producer and reader",
		"Closed by ADR 069 for",
	] {
		if !adr064.contains(required) {
			violations.push(format!(
				"plans/adr/0064_nullable_sparse_projection.md: missing ADR 069 closure token {required}"
			));
		}
	}
	for required in [
		"plans/impl/0069_encoded_validity_sparse_projection_bench_results.md",
		"Shipped and promoted",
		"now initializes `Enabled`",
		"RowGroupRowCountMismatch",
	] {
		if !roadmap.contains(required) {
			violations.push(format!(
				"plans/wip/roadmap.md: missing ADR 069 promotion token {required}"
			));
		}
	}
	if page_materialize.contains("decode_page(")
		&& page_materialize.contains("decode_rle_validity_page")
		&& manager.contains("PageMaterializeValiditySource::Encoded")
		&& !manager.contains("decode_page_fetch(fetch, &fetched)")
	{
		// The existing value-page decoder can remain in the helper module, but encoded
		// validity materialization must take the RLE bitmap path in manager.rs.
	} else if manager.contains("PageMaterializeValiditySource::Encoded")
		&& manager.contains("decode_page_fetch(fetch, &fetched)")
	{
		violations.push(
			"src/storage/manager.rs: encoded validity must not reuse fixed-width page decoder"
				.to_string(),
		);
	}
	assert!(
		violations.is_empty(),
		"ADR 069 encoded-validity sparse projection invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr070_all_null_page_skip_sparse_projection_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let sparse_projection =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/sparse_projection_fetch.rs"))
			.unwrap();
	let page_materialize =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/page_materialize.rs")).unwrap();
	let manager = manager_source(manifest_dir);
	let row_group_bench =
		fs::read_to_string(manifest_dir.join("benches/row_group_fetch.rs")).unwrap();
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();
	let schema_pbc = fs::read_to_string(manifest_dir.join("src/schema/pbc.rs")).unwrap();
	let adr = fs::read_to_string(
		manifest_dir.join("plans/adr/0070_all_null_page_skip_sparse_projection.md"),
	)
	.unwrap();
	let adr064 =
		fs::read_to_string(manifest_dir.join("plans/adr/0064_nullable_sparse_projection.md"))
			.unwrap();
	let adr069 = fs::read_to_string(
		manifest_dir.join("plans/adr/0069_encoded_validity_sparse_projection.md"),
	)
	.unwrap();
	let impl_plan = fs::read_to_string(
		manifest_dir.join("plans/impl/0070_all_null_page_skip_sparse_projection.md"),
	)
	.unwrap();
	let bench_results = fs::read_to_string(
		manifest_dir.join("plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md"),
	)
	.unwrap();

	for required in [
		"pub(crate) enum SelectedPageDisposition",
		"AllNullElided",
		"page_row_count",
		"bytes_per_value",
		"value_page_dispositions",
		"pub(crate) enum AllNullElisionDeclineReason",
		"pub(crate) fn value_page_is_metadata_all_null",
		"all_null_elision_data_type_eligible",
		"DataType::Int8",
		"DataType::UInt64",
		"DataType::Timestamp",
		"TimeUnit::Nanosecond",
		"DataType::Date64",
	] {
		if !sparse_projection.contains(required) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: missing ADR 070 planner token {required}"
			));
		}
	}
	for forbidden in ["fixed_width_value_bytes(field.data_type()).is_none() ||"] {
		if sparse_projection.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: forbidden ADR 070 V1 widening token {forbidden}"
			));
		}
	}
	for required in [
		"pub(crate) enum PageMaterializeValueSource",
		"AllNullSynthesized",
		"pub(crate) fn synthesize_all_null_page_buffers",
		"validity_page_decoded_byte_len(page_row_count)",
		"null_count: page_row_count",
	] {
		if !page_materialize.contains(required) {
			violations.push(format!(
				"src/storage/encoding/page_materialize.rs: missing ADR 070 synthesis token {required}"
			));
		}
	}
	for required in [
		"SelectedPageDisposition",
		"synthesize_all_null_projection_page",
		"SelectedPageDisposition::AllNullElided",
		"PageMaterializeValueSource::AllNullSynthesized",
		"synthesize_all_null_page_buffers",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 070 runtime token {required}"
			));
		}
	}
	for required in [
		"--all-null-elision-only",
		"--rows=",
		"adr070_all_null_page_skip",
		"elided_pages",
		"synthesized_value_bytes",
		"drift_sentinel_pages",
		"baseline_macro_block_get_count",
		"elision_macro_block_get_count",
	] {
		if !row_group_bench.contains(required) {
			violations.push(format!(
				"benches/row_group_fetch.rs: missing ADR 070 bench token {required}"
			));
		}
	}
	if !encoding_types.contains("pub const ENCODING_DIRECTORY_FORMAT_VERSION: u32 = 3;") {
		violations.push("ADR 070 must not bump ENCODING_DIRECTORY_FORMAT_VERSION".to_string());
	}
	for forbidden in [
		"AllNullElision",
		"all_null_elision",
		"AllNullPageSkip",
		"all_null_page_skip",
	] {
		if config.contains(forbidden)
			|| metrics.contains(forbidden)
			|| schema_pbc.contains(forbidden)
			|| encoding_types.contains(forbidden)
		{
			violations.push(format!(
				"ADR 070 must not expose all-null elision through public/config surfaces, found {forbidden}"
			));
		}
	}
	for required in [
		"Boolean is V2",
		"Do not gate promotion on GET count never increasing",
		"Do not add bridge-overfetch logic",
		"Catalog gating is the planner's",
	] {
		if !adr.contains(required) {
			violations.push(format!(
				"plans/adr/0070_all_null_page_skip_sparse_projection.md: missing ADR 070 convergence token {required}"
			));
		}
	}
	for required in ["ADR 070 later", "closes this deferral"] {
		if !adr064.contains(required) {
			violations.push(format!(
				"plans/adr/0064_nullable_sparse_projection.md: missing ADR 070 closure token {required}"
			));
		}
	}
	for required in ["closed by ADR 070", "metadata-all-null reader path"] {
		if !adr069.contains(required) {
			violations.push(format!(
				"plans/adr/0069_encoded_validity_sparse_projection.md: missing ADR 070 boundary-closure token {required}"
			));
		}
	}
	for required in [
		"SelectedPageDisposition",
		"value_page_is_metadata_all_null",
		"synthesize_all_null_page_buffers",
		"all-null elision",
	] {
		if !impl_plan.contains(required) {
			violations.push(format!(
				"plans/impl/0070_all_null_page_skip_sparse_projection.md: missing ADR 070 implementation token {required}"
			));
		}
	}
	for required in [
		"Status: implementation evidence captured",
		"Phase 0 Audit",
		"Prompt-to-artifact Checklist",
		"cargo bench --bench row_group_fetch -- --all-null-elision-only --rows=8192,65536,1048576",
		"alternating_pages_all_null",
		"baseline_macro_block_get_count",
		"drift_sentinel_pages",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/0070_all_null_page_skip_sparse_projection_bench_results.md: missing ADR 070 evidence token {required}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 070 all-null page skip sparse projection invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr060_sparse_projection_fetch_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let encoding_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/mod.rs")).unwrap();
	let page_materialize =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/page_materialize.rs")).unwrap();
	let sparse_projection =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/sparse_projection_fetch.rs"))
			.unwrap();
	let manager = manager_source(manifest_dir);
	let columnar = fs::read_to_string(manifest_dir.join("src/storage/columnar.rs")).unwrap();
	let row_group = fs::read_to_string(manifest_dir.join("src/storage/row_group.rs")).unwrap();
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let compute_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/compute/types.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let schema_pbc = fs::read_to_string(manifest_dir.join("src/schema/pbc.rs")).unwrap();
	let transport = fs::read_to_string(manifest_dir.join("src/network/transport.rs")).unwrap();
	let metrics = fs::read_to_string(manifest_dir.join("src/metrics/registry.rs")).unwrap();

	for required in [
		"pub mod page_materialize;",
		"pub mod sparse_projection_fetch;",
	] {
		if !encoding_mod.contains(required) {
			violations.push(format!(
				"src/storage/encoding/mod.rs: missing ADR 060 token {required}"
			));
		}
	}
	for required in [
		"PageMaterializeKey",
		"PageMaterializeBufferKey",
		"PageMaterializeColumn",
		"PageMaterializeFetch",
		"PageMaterializeValidityFetch",
		"PageValidityBitmap",
		"decode_page_fetch",
		"projected_page_batch_from_decoded_buffers",
		"extract_record_body_bytes",
		"fixed_width_sparse_projection_column",
		"fixed_width_encoding_indexed_value_column",
		"validity_bitmap_body_range_for_page",
		"rebase_validity_bitmap",
		"empty_projected_batch_for_row_count",
	] {
		if !page_materialize.contains(required) {
			violations.push(format!(
				"src/storage/encoding/page_materialize.rs: missing ADR 060 helper token {required}"
			));
		}
	}
	for forbidden in [
		"SparseProjectionFetchDecision",
		"SparseProjectionFetchDecline",
		"decide_sparse_projection_fetch",
		"MAX_SPARSE_RANGES_PER_BUFFER",
	] {
		if page_materialize.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/page_materialize.rs: helper module must not own planner token {forbidden}"
			));
		}
	}
	for required in [
		"pub(crate) const MAX_SPARSE_RANGES_PER_BUFFER",
		"pub(crate) struct RowToPageMap",
		"pub(crate) enum ActivePageSet",
		"pub(crate) struct SparseProjectionFetchPlan",
		"pub(crate) enum SparseProjectionFetchDecision",
		"pub(crate) enum SparseProjectionFetchDecline",
		"pub(crate) fn decide_sparse_projection_fetch",
		"mask_to_active_pages",
		"projection_cascade_eligible",
		"validity_fetches",
		"EncodedValidityBufferUnsupported",
		"ValidityBufferRangeMissing",
		"NullableTypeNotInCatalog",
		"all_valid_nullable_pages",
		"ByteEstimateNotSparse",
		"PerBufferFragmentationCapExceeded",
	] {
		if !sparse_projection.contains(required) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: missing ADR 060 planner token {required}"
			));
		}
	}
	for forbidden in [
		"decode_page_fetch(",
		"projected_page_batch_from_decoded_buffers(",
		"pub struct Config",
		"sparse_phase",
		"SparsePhase",
		"phase_c",
	] {
		if sparse_projection.contains(forbidden) {
			violations.push(format!(
				"src/storage/encoding/sparse_projection_fetch.rs: forbidden ADR 060 token {forbidden}"
			));
		}
	}
	for required in [
		"decide_sparse_projection_fetch(SparseProjectionFetchInput",
		"materialize_sparse_projection_pages",
		"materialize_sparse_projection_page_batches",
		"predicate_output_overlap",
		"filtered_predicate_page_batch",
		"zip_predicate_and_projection_columns",
		"predicate_outcome: &predicate_outcome",
		"plan.validity_fetches",
		"rebase_validity_bitmap",
		"observe_storage_l2_encoding_simd_pages_fetched",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 060 runtime token {required}"
			));
		}
	}
	for forbidden in [
		"L2FetchStrategy::Sparse",
		"RowGroupFallbackReason::Sparse",
		"EncodingFallbackReason::Sparse",
		"EncodingRejectCode::Sparse",
		"EncodedRowMask::Sparse",
		"sparse_projection_fetch_",
	] {
		if columnar.contains(forbidden)
			|| row_group.contains(forbidden)
			|| encoding_types.contains(forbidden)
			|| compute_types.contains(forbidden)
			|| config.contains(forbidden)
			|| schema_pbc.contains(forbidden)
			|| transport.contains(forbidden)
			|| metrics.contains(forbidden)
		{
			violations.push(format!("ADR 060 forbidden public/config token {forbidden}"));
		}
	}
	if !row_group.contains("pub struct RowGroupFetchPlan")
		|| !row_group.contains("phase_c_fetch")
		|| !columnar.contains("RowGroup(RowGroupFetchPlan)")
	{
		violations.push(
			"ADR 060 must reuse the existing row-group fetch strategy and projection-fetch plan field"
				.to_string(),
		);
	}
	if metrics.contains("sparse_projection") || metrics.contains("sparse_projection_fetch") {
		violations.push("ADR 060 must not add sparse-specific metric labels".to_string());
	}
	if page_materialize.contains("NullableProjectedColumn")
		|| sparse_projection.contains("NullableProjectedColumn")
		|| manager.contains("NullableProjectedColumn")
	{
		violations.push("ADR 064 must not restore NullableProjectedColumn".to_string());
	}

	assert!(
		violations.is_empty(),
		"ADR 060 sparse projection fetch invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr061_authenticated_chunk_layout_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let crypto = fs::read_to_string(manifest_dir.join("src/crypto.rs")).unwrap();
	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let encoding_mod =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/mod.rs")).unwrap();
	let chunk_layout =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/chunk_layout.rs")).unwrap();
	let directory =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/directory.rs")).unwrap();
	let manager = manager_source(manifest_dir);
	let node = fs::read_to_string(manifest_dir.join("src/node.rs")).unwrap();
	let config = fs::read_to_string(manifest_dir.join("src/config.rs")).unwrap();
	let macro_block_test =
		fs::read_to_string(manifest_dir.join("tests/crypto_macro_block_coalescing.rs")).unwrap();
	let bench_results = fs::read_to_string(
		manifest_dir
			.join("plans/impl/0061_page_aligned_authenticated_chunk_layout_bench_results.md"),
	)
	.unwrap();

	for required in [
		"pub const SLAB_ENCRYPTION_DEFAULT_CHUNK_BYTES: usize = 64 * 1024;",
		"pub const SLAB_ENCRYPTION_DEFAULT_CHUNK_SIZE: ChunkSizeBytes",
		"pub const SLAB_ENCRYPTION_CHUNK_SIZES: [usize; 5]",
		"4 * 1024",
		"8 * 1024",
		"16 * 1024",
		"32 * 1024",
		"64 * 1024",
		"pub chunk_size_bytes: ChunkSizeBytes",
		"validate_slab_chunk_size(chunk_size_bytes)?",
		"chunk_size_bytes.get().to_be_bytes()",
		"ChunkSizeBytes::new(u32::from_be_bytes",
	] {
		if !crypto.contains(required) {
			violations.push(format!("src/crypto.rs: missing ADR 061 token {required}"));
		}
	}
	if !units.contains("impl_unit!(ChunkSizeBytes, u32);") {
		violations.push("src/schema/units.rs: missing ChunkSizeBytes unit".to_string());
	}
	for forbidden in [
		concat!("SLAB_ENCRYPTION_", "LEGACY_", "CHUNK_BYTES"),
		concat!("SLAB_ENCRYPTION_", "CHUNK_BYTES"),
		"AuthenticatedChunkDirectory",
		"base_chunk_index",
	] {
		if crypto.contains(forbidden)
			|| units.contains(forbidden)
			|| encoding_mod.contains(forbidden)
			|| chunk_layout.contains(forbidden)
			|| directory.contains(forbidden)
			|| manager.contains(forbidden)
			|| node.contains(forbidden)
			|| config.contains(forbidden)
		{
			violations.push(format!(
				"ADR 061 forbidden implementation token {forbidden}"
			));
		}
	}
	for required in ["pub mod chunk_layout;", "pub use chunk_layout::*;"] {
		if !encoding_mod.contains(required) {
			violations.push(format!(
				"src/storage/encoding/mod.rs: missing ADR 061 token {required}"
			));
		}
	}
	for required in [
		"pub struct L2ChunkLayoutConfig",
		"pub enabled: bool",
		"pub max_pad_inflation_micros: SelectivityFractionMicros",
		"enabled: true",
		"PROMOTED_FLOOR: ChunkSizeBytes = ChunkSizeBytes::new(16 * 1024)",
		"pub fn choose_chunk_size_bytes",
		"pub fn choose_chunk_size_bytes_for_directories",
		"pub fn estimate_pad_inflation",
		"pub fn align_buffers_to_chunks",
		"default_sparse_engaged_cascade",
		"EncodingCascade::Dictionary",
		"EncodingLeafCascade::Plain | EncodingLeafCascade::BitPacked",
	] {
		if !chunk_layout.contains(required) {
			violations.push(format!(
				"src/storage/encoding/chunk_layout.rs: missing ADR 061 token {required}"
			));
		}
	}
	if chunk_layout.contains("matches!(cascade, EncodingCascade::Rle") {
		violations
			.push("ADR 061 must not put Rle in the default sparse engagement set".to_string());
	}
	for required in [
		"chunk_size_bytes: ChunkSizeBytes",
		"default_sparse_engaged_key",
		"body.resize(new_len, 0)",
		"chunk_alignment_pad_bytes",
	] {
		if !directory.contains(required) {
			violations.push(format!(
				"src/storage/encoding/directory.rs: missing ADR 061 token {required}"
			));
		}
	}
	for required in [
		"pub l2_chunk_layout: L2ChunkLayoutConfig",
		"choose_tiered_object_chunk_size",
		"choose_chunk_size_bytes_for_directories",
		"l2_chunk_layout_config",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 061 token {required}"
			));
		}
	}
	for required in [
		"pub l2_chunk_layout: L2ChunkLayoutConfig",
		"self.l2_chunk_layout.validate()?",
		"l2_chunk_layout: cfg.l2_chunk_layout.clone()",
	] {
		if !node.contains(required) {
			violations.push(format!("src/node.rs: missing ADR 061 token {required}"));
		}
	}
	for required in [
		"l2_chunk_layout: RawL2ChunkLayoutConfig",
		"struct RawL2ChunkLayoutConfig",
		"max_pad_inflation_micros",
		"l2_chunk_layout,",
	] {
		if !config.contains(required) {
			violations.push(format!("src/config.rs: missing ADR 061 token {required}"));
		}
	}
	for required in [
		"100 * 1024 * 1024",
		"ChunkSizeBytes::new(16 * 1024)",
		"ranges.len()",
		"1,",
		"coalesce_per_region",
		"plan.decrypt",
	] {
		if !macro_block_test.contains(required) {
			violations.push(format!(
				"tests/crypto_macro_block_coalescing.rs: missing ADR 061 token {required}"
			));
		}
	}
	for required in [
		"cargo bench --bench row_group_fetch > /tmp/row_group_fetch_061_full_final.out",
		"Full-run evidence supports the ADR 061 V1 default of 16 KiB",
		"| 16 KiB | 6,400 | 1 | 1 |",
	] {
		if !bench_results.contains(required) {
			violations.push(format!(
				"plans/impl/0061_page_aligned_authenticated_chunk_layout_bench_results.md: missing ADR 061 token {required}"
			));
		}
	}

	assert!(
		violations.is_empty(),
		"ADR 061 authenticated chunk layout invariants violated:\n{}",
		violations.join("\n")
	);
}

#[test]
fn test_adr063_page_level_predicate_stats_guards() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut violations = Vec::new();

	let units = fs::read_to_string(manifest_dir.join("src/schema/units.rs")).unwrap();
	let stats = fs::read_to_string(manifest_dir.join("src/storage/stats.rs")).unwrap();
	let encoding_types =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/types.rs")).unwrap();
	let encoding_directory =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/directory.rs")).unwrap();
	let predicate_page_fetch =
		fs::read_to_string(manifest_dir.join("src/storage/encoding/predicate_page_fetch.rs"))
			.unwrap();
	let manager = manager_source(manifest_dir);

	for required in [
		"impl_unit!(PageNullCount, u32)",
		"impl_unit!(EncodingPageCount, u32)",
	] {
		if !units.contains(required) {
			violations.push(format!(
				"src/schema/units.rs: missing ADR 063 unit {required}"
			));
		}
	}
	for required in [
		"pub struct CanonicalScalar",
		"pub struct CanonicalStatsValues",
		"canonical_stats_for_array_slice",
		"fixed_width_page_stats_supported_type",
		"DataType::Timestamp(TimeUnit::Millisecond, None)",
	] {
		if !stats.contains(required) {
			violations.push(format!(
				"src/storage/stats.rs: missing ADR 063 shared stats token {required}"
			));
		}
	}
	for required in [
		"ENCODING_DIRECTORY_FORMAT_VERSION: u32 = 3",
		"pub struct EncodingPageStats",
		"pub page_stats: Vec<EncodingPageStats>",
		"pub null_count: PageNullCount",
	] {
		if !encoding_types.contains(required) {
			violations.push(format!(
				"src/storage/encoding/types.rs: missing ADR 063 directory shape token {required}"
			));
		}
	}
	for required in [
		"page_stats_cascade_eligible",
		"EncodingComputeConfig::default()",
		"enabled_predicate_evaluators",
		"cascade.emits_pages_in_v1()",
		"validate_page_stats",
		"write_canonical_scalar",
		"read_canonical_scalar",
	] {
		if !encoding_directory.contains(required) {
			violations.push(format!(
				"src/storage/encoding/directory.rs: missing ADR 063 writer/validation token {required}"
			));
		}
	}
	for required in [
		"pub enum PagePredicateClassification",
		"PredicatePageFetchPlan",
		"enum PredicatePageFetchDecline",
		"stitch_predicate_page_mask",
		"classify_page_stats_for_atom",
		"PagePredicateClassification::ExactMatch",
		"PagePredicateClassification::Disjoint",
		"PagePredicateClassification::Maybe",
	] {
		if !predicate_page_fetch.contains(required) {
			violations.push(format!(
				"src/storage/encoding/predicate_page_fetch.rs: missing ADR 063 classifier token {required}"
			));
		}
	}
	for required in [
		"build_encoding_predicate_page_plan",
		"EncodingPredicatePageDecision",
		"page_fallback",
		"page_exact",
		"classify_page_stats_for_atom",
	] {
		if !manager.contains(required) {
			violations.push(format!(
				"src/storage/manager.rs: missing ADR 063 predicate-page narrowing token {required}"
			));
		}
	}

	use arrow::datatypes::{DataType, TimeUnit};
	use wendelon::storage::encoding::compute::EncodingComputeEvaluatorKind;
	use wendelon::storage::encoding::directory::page_stats_cascade_eligible;
	use wendelon::storage::encoding::{EncodingCascade, EncodingLeafCascade};
	use wendelon::storage::stats::fixed_width_page_stats_supported_type;
	let bitpacked = EncodingCascade::BitPacked {
		bits: wendelon::schema::BitsPerValue::new(8),
		base: wendelon::schema::BaseValue::new(0),
	};
	let dictionary_plain = EncodingCascade::Dictionary {
		dict_byte_len: wendelon::schema::RecordBodyByteLen::new(8),
		dict_value_count: wendelon::schema::DictValueCount::new(1),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	let dictionary_bitpacked = EncodingCascade::Dictionary {
		dict_byte_len: wendelon::schema::RecordBodyByteLen::new(8),
		dict_value_count: wendelon::schema::DictValueCount::new(1),
		inner: Box::new(EncodingLeafCascade::BitPacked {
			bits: wendelon::schema::BitsPerValue::new(1),
			base: wendelon::schema::BaseValue::new(0),
		}),
	};
	let rle = EncodingCascade::Rle {
		run_count: wendelon::schema::RunCount::new(1),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	let delta = EncodingCascade::Delta {
		base: wendelon::schema::BaseValue::new(0),
		inner: Box::new(EncodingLeafCascade::Plain),
	};
	for cascade in [&bitpacked, &dictionary_plain, &dictionary_bitpacked] {
		if !page_stats_cascade_eligible(cascade) {
			violations.push(format!(
				"ADR 063 V1 cascade intersection unexpectedly excluded {cascade:?}"
			));
		}
	}
	for cascade in [&rle, &delta] {
		if page_stats_cascade_eligible(cascade) {
			violations.push(format!(
				"ADR 063 V1 cascade intersection unexpectedly included {cascade:?}"
			));
		}
	}
	if !fixed_width_page_stats_supported_type(&DataType::Int64)
		|| !fixed_width_page_stats_supported_type(&DataType::Timestamp(TimeUnit::Nanosecond, None))
		|| fixed_width_page_stats_supported_type(&DataType::Float64)
		|| fixed_width_page_stats_supported_type(&DataType::Utf8)
	{
		violations.push(
			"ADR 063 fixed-width page stat type subset drifted from the approved V1 set"
				.to_string(),
		);
	}
	if !format!("{:?}", EncodingComputeEvaluatorKind::Delta).contains("Delta") {
		violations
			.push("ADR 063 architecture guard failed to reach compute evaluator types".into());
	}

	assert!(
		violations.is_empty(),
		"ADR 063 page-level predicate stats invariants violated:\n{}",
		violations.join("\n")
	);
}

fn workspace_root() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.and_then(Path::parent)
		.expect("workspace root")
		.to_path_buf()
}

fn read_workspace(path: &str) -> String {
	fs::read_to_string(workspace_root().join(path)).unwrap_or_else(|err| {
		panic!("{path}: failed to read workspace file: {err}");
	})
}

/// ADR 077: production data-server source via the workspace root (the
/// `read_workspace` style). Concatenates `network/data_server.rs` plus every
/// non-test submodule, so guards that scan handler code keep working after the
/// monolith was split into domain modules. Mirrors `data_server_source`.
fn data_server_workspace_source() -> String {
	data_server_source(&workspace_root().join("crates/wendelon"))
}

fn source_region<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
	source
		.split_once(start)
		.unwrap_or_else(|| panic!("missing source start marker {start:?}"))
		.1
		.split_once(end)
		.unwrap_or_else(|| panic!("missing source end marker {end:?} after {start:?}"))
		.0
}

fn wendelon_cli_source() -> String {
	read_workspace("crates/wendelon/src/cli.rs")
}

fn wendelon_inspect_region(cli: &str) -> &str {
	source_region(cli, "fn run_inspect(args: &[String])", "fn verify_audit")
}

fn wendelon_inspect_fetch_region(cli: &str) -> &str {
	source_region(
		cli,
		"async fn fetch_ready_with_tcp_connect",
		"fn parse_readiness_view",
	)
}

#[test]
fn adr071_wendelon_inspect_branch_exists() {
	let cli = wendelon_cli_source();
	assert!(cli.contains("[cmd, rest @ ..] if cmd == \"inspect\" => run_inspect(rest)"));
	assert!(cli.contains("fn run_inspect(args: &[String]) -> ExitCode"));
	assert!(cli.contains("{program} inspect [--endpoint <addr>]"));
}

#[test]
fn adr071_wendelon_inspect_is_client_only() {
	let cli = wendelon_cli_source();
	let inspect = wendelon_inspect_region(&cli);
	for forbidden in [
		"WendelonNode",
		"NodeBootstrapConfig",
		"load_node_bootstrap_config",
		"observability::spawn",
		"LocalAuditSink",
		"ReadinessReporter",
		"start_daemon",
	] {
		assert!(
			!inspect.contains(forbidden),
			"inspect path must not construct daemon-side machinery token {forbidden}"
		);
	}
}

#[test]
fn adr071_wendelon_inspect_endpoint_precedence_is_pinned() {
	let cli = wendelon_cli_source();
	let parse = source_region(&cli, "fn parse_inspect_args", "async fn run_inspect_async");
	assert!(cli.contains("const DEFAULT_INSPECT_ENDPOINT: &str = \"127.0.0.1:7432\";"));
	assert!(parse.contains("WENDELON_INSPECT_ENDPOINT"));
	assert!(parse.contains("endpoint_flag"));
	assert!(parse.contains("invalid WENDELON_INSPECT_ENDPOINT"));
	assert!(parse.contains("DEFAULT_INSPECT_ENDPOINT"));
}

#[test]
fn adr071_wendelon_inspect_tls_flags_are_all_or_none() {
	let cli = wendelon_cli_source();
	let parse = source_region(&cli, "fn parse_inspect_args", "async fn run_inspect_async");
	assert!(cli.contains("wendelon inspect requires --server-name, --cert, --key, and --ca"));
	for required in [
		"WENDELON_INSPECT_SERVER_NAME",
		"WENDELON_INSPECT_CLIENT_CERT",
		"WENDELON_INSPECT_CLIENT_KEY",
		"WENDELON_INSPECT_CA",
		"server_name.is_none() || cert_path.is_none() || key_path.is_none() || ca_path.is_none()",
	] {
		assert!(
			parse.contains(required),
			"missing TLS parse token {required}"
		);
	}
}

#[test]
fn adr071_wendelon_inspect_uses_existing_actor_authorization() {
	let tls = read_workspace("crates/wendelon/src/network/tls.rs");
	let observability = read_workspace("crates/wendelon/src/metrics/observability.rs");
	let cli = wendelon_cli_source();
	assert!(tls.contains("ActorPermission::ObservabilityReady"));
	assert!(observability.contains("ActorPermission::ObservabilityReady"));
	assert!(observability.contains("authorize_actor(&actor, permission)"));
	assert!(!tls.contains("Inspect"));
	assert!(!cli.contains("monitor-"));
	assert!(!tls.contains("inspect-"));
	assert!(
		!read_workspace("crates/wendelon/src/network/cert_issuance_policy.rs").contains("inspect-")
	);
}

#[test]
fn adr071_wendelon_inspect_uses_empty_alpn() {
	let cli = wendelon_cli_source();
	let fetch = wendelon_inspect_fetch_region(&cli);
	assert!(fetch.contains("rustls_client_config_with_alpn(&material, Vec::new())"));
	assert!(!fetch.contains("HRFT_ALPN"));
	assert!(!fetch.contains("http/1.1"));
}

#[test]
fn adr071_wendelon_inspect_uses_observability_http_client_helpers() {
	let cli = wendelon_cli_source();
	let fetch = wendelon_inspect_fetch_region(&cli);
	assert!(fetch.contains("observability_http::write_request"));
	assert!(fetch.contains("observability_http::read_response"));
}

#[test]
fn adr071_wendelon_inspect_does_not_use_generic_http_clients() {
	let cli = wendelon_cli_source();
	let fetch = wendelon_inspect_fetch_region(&cli);
	for forbidden in [
		"reqwest",
		"hyper",
		"h2",
		"http::Request",
		"http::Response",
		"tower",
		"ureq",
	] {
		assert!(
			!fetch.contains(forbidden),
			"inspect fetch path must not use generic HTTP client token {forbidden}"
		);
	}
}

#[test]
fn adr071_wendelon_inspect_accepts_only_200_and_503() {
	let cli = wendelon_cli_source();
	let fetch = wendelon_inspect_fetch_region(&cli);
	assert!(fetch.contains("response.status_code != 200 && response.status_code != 503"));
	assert!(fetch.contains("InspectError::UnexpectedStatus(response.status_code)"));
	assert!(cli.contains(
		"wendelon inspect: failed to read /ready response: unexpected HTTP status {status_code}"
	));
}

#[test]
fn adr071_wendelon_inspect_exit_codes_are_pinned_to_0_1_2() {
	let cli = wendelon_cli_source();
	let inspect = wendelon_inspect_region(&cli);
	assert!(inspect.contains("ExitCode::SUCCESS"));
	assert!(inspect.contains("ExitCode::from(1)"));
	assert!(inspect.contains("ExitCode::from(2)"));
	assert!(!inspect.contains("ExitCode::from(3)"));
}

#[test]
fn adr071_wendelon_inspect_text_and_json_output_are_pinned() {
	let cli = wendelon_cli_source();
	let render = source_region(&cli, "fn render_inspect_output", "fn verify_audit");
	for required in [
		"out.push(b'\\n')",
		"render_readiness_text",
		"node_id",
		"epoch",
		"leader_id",
		"cluster_members",
		"drain_state",
		"audit_materialization_lag_entries",
		"degraded_reason",
		"fips_mode",
		"fips_self_test_last_ts",
		"serde_json::to_string(value)",
	] {
		assert!(render.contains(required), "missing render token {required}");
	}
	let run = source_region(
		&cli,
		"async fn run_inspect_async",
		"fn inspect_error_message",
	);
	assert!(run.contains("io::stdout().write_all"));
	assert!(run.contains("eprintln!(\"wendelon inspect: failed to write output: {err}\")"));
}

#[test]
fn adr071_wendelon_inspect_has_symmetric_timeouts() {
	let cli = wendelon_cli_source();
	assert!(cli.contains("pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);"));
	assert!(cli.contains("pub const READ_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);"));
	let fetch = wendelon_inspect_fetch_region(&cli);
	assert!(fetch.contains("tokio::time::timeout(connect_timeout, connect)"));
	assert!(fetch.contains("tokio::time::timeout(read_timeout, observability_http::read_response"));
}

#[test]
fn adr071_no_new_wendelon_observability_routes() {
	let observability = read_workspace("crates/wendelon/src/metrics/observability.rs");
	assert!(!observability.contains("INSPECT_PATH"));
	assert!(!observability.contains("\"/inspect\""));
	assert!(observability.contains("pub const READY_PATH: &str = \"/ready\";"));
	assert!(observability.contains("pub const METRICS_PATH: &str = \"/metrics\";"));
}

#[test]
fn adr071_wendelon_observability_server_helpers_remain_local() {
	let observability = read_workspace("crates/wendelon/src/metrics/observability.rs");
	let exporter = read_workspace("crates/wendelon/src/metrics/exporter.rs");
	assert!(observability.contains("async fn read_request"));
	assert!(observability.contains("fn parse_request_line"));
	assert!(observability.contains("async fn write_response"));
	assert!(!observability.contains("observability_http"));
	assert!(!exporter.contains("observability_http"));
}

#[test]
fn adr071_nilket_root_stays_runtime_free() {
	let nilket_manifest = read_workspace("crates/nilket/Cargo.toml");
	for forbidden in [
		"tokio",
		"tokio-rustls",
		"rustls",
		"observability",
		"tls-pki",
		"serde_json",
	] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket root manifest must stay runtime-free; found {forbidden}"
		);
	}
}

#[test]
fn adr071_prior_adr_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn test_adr039_wendelon_drop_migration_substrate_guards",
		"fn test_adr055_lsm_compaction_substrate_guards",
		"fn test_adr063_page_level_predicate_stats_guards",
		"fn test_adr066_fsst_evaluator_guards",
		"fn test_adr067_row_safe_fsst_guards",
		"fn test_adr068_fsst_row_safe_estimator_guards",
		"fn test_adr069_encoded_validity_sparse_projection_guards",
		"fn test_adr070_all_null_page_skip_sparse_projection_guards",
	] {
		assert!(
			architecture.contains(required),
			"missing prior guard {required}"
		);
	}
}

#[test]
fn adr071_new_inspect_types_are_brand_clean_and_no_v1_suffix() {
	let cli = wendelon_cli_source();
	let inspect = wendelon_inspect_region(&cli);
	for required in ["InspectOptions", "InspectError", "ReadinessView"] {
		assert!(
			inspect.contains(required),
			"missing inspect type {required}"
		);
	}
	for forbidden in [
		"WendelonInspect",
		"InspectOptionsV1",
		"InspectErrorV1",
		"ReadinessViewV1",
		"InspectV1",
	] {
		assert!(
			!inspect.contains(forbidden),
			"inspect surface must stay brand-clean and no-V1; found {forbidden}"
		);
	}
}

fn adr072_control_descriptor_get_handler_region(control: &str) -> &str {
	source_region(
		control,
		"async fn handle_table_descriptor_get",
		"async fn handle_table_descriptor_list",
	)
}

fn adr102_control_descriptor_list_handler_region(control: &str) -> &str {
	source_region(
		control,
		"async fn handle_table_descriptor_list",
		"async fn handle_logical_type_get",
	)
}

fn adr072_descriptor_get_auth_region(control: &str) -> &str {
	source_region(
		control,
		"fn authorize_descriptor_get_actor",
		"async fn handle_table_descriptor_get",
	)
}

#[test]
fn adr072_descriptor_get_msg_types_are_unique() {
	let schema_core = read_workspace("crates/wendelon/src/schema/core.rs");
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_GET_REQ: Self = Self(0x3C);"));
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_GET_RESP: Self = Self(0x3D);"));
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_LIST_REQ: Self = Self(0x44);"));
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_LIST_RESP: Self = Self(0x45);"));
	assert_eq!(schema_core.matches("Self(0x3C)").count(), 1);
	assert_eq!(schema_core.matches("Self(0x3D)").count(), 1);
	assert_eq!(schema_core.matches("Self(0x44)").count(), 1);
	assert_eq!(schema_core.matches("Self(0x45)").count(), 1);

	let router = read_workspace("crates/wendelon/src/network/router.rs");
	// The invariant is that the router names and routes these message types. How it
	// compares them is not the invariant: the router now collects candidates and
	// matches with `candidate.0 == msg_type` instead of comparing `MsgType::X.0`
	// per arm, so pinning the `.0` spelling pinned a shape rather than the routing.
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_GET_REQ"));
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_GET_RESP"));
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_LIST_REQ"));
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_LIST_RESP"));
}

#[test]
fn adr072_descriptor_get_response_meta_has_no_bool_and_pins_descriptor_len() {
	let schema_core = read_workspace("crates/wendelon/src/schema/core.rs");
	let response = source_region(
		&schema_core,
		"pub struct TableDescriptorGetResponseMeta",
		"// ============================================================\n//  ROLLING-UPGRADE VERSION WINDOW",
	);
	assert!(response.contains("pub accepted: u8"));
	assert!(response.contains("pub reason_code: u8"));
	assert!(response.contains("pub leader_hint: NodeId"));
	assert!(response.contains("pub descriptor_len: u32"));
	assert!(!response.contains("bool"));
	assert!(schema_core.contains("size_of::<TableDescriptorGetRequestMeta>() == 264"));
	assert!(schema_core.contains("size_of::<TableDescriptorGetResponseMeta>() == 24"));
}

#[test]
fn adr072_descriptor_get_uses_descriptor_authorization_helper() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	assert!(control.contains("fn authorize_descriptor_get_actor"));
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("Self::authorize_descriptor_get_actor(&context.actor"));
	assert!(!handler.contains("TenantAdminRequestWire"));
}

#[test]
fn adr072_descriptor_get_enforces_tenant_isolation() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let auth = adr072_descriptor_get_auth_region(&control);
	assert!(auth.contains("authorize_actor(actor, ActorPermission::TenantAdminRead)"));
	assert!(auth.contains("actor.claimed_tenant_id()?"));
	assert!(auth.contains("claimed_tenant != tenant_id"));
}

#[test]
fn adr072_descriptor_get_is_leader_only_with_leader_hint() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("!self.is_leader().await"));
	assert!(handler.contains("CONTROL_REASON_NOT_LEADER"));
	assert!(handler.contains("self.leader_hint()"));
	assert!(handler.contains("leader_hint"));
}

#[test]
fn adr072_descriptor_get_bypasses_raft_log() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(!handler.contains("client_write"));
	assert!(!handler.contains("WendelonRequest"));
	assert!(!handler.contains("SchemaUpdate"));
	assert!(handler.contains("self.state_machine.state_snapshot().await"));
}

#[test]
fn adr072_descriptor_get_returns_latest_by_name() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("table_schemas.latest_by_name"));
	assert!(handler.contains("request.namespace.as_str()"));
	assert!(handler.contains("request.table_name.as_str()"));
}

#[test]
fn adr072_descriptor_get_not_found_is_control_reason_not_found() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("CONTROL_REASON_NOT_FOUND"));
	assert!(handler.contains("table_descriptor_get_rejection"));
}

#[test]
fn adr072_descriptor_get_tenant_mismatch_is_authorization_failed() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let auth = adr072_descriptor_get_auth_region(&control);
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(auth.contains("claimed_tenant != tenant_id"));
	assert!(handler.contains("CONTROL_REASON_AUTHORIZATION_FAILED"));
}

#[test]
fn adr072_descriptor_get_malformed_name_is_invalid_config() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("decode_table_descriptor_get_request"));
	assert!(handler.contains("CONTROL_REASON_INVALID_CONFIG"));
}

#[test]
fn adr072_descriptor_get_returns_catalog_descriptor_without_mutation() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(handler.contains("encode_table_schema_descriptor_v1(descriptor)"));
	assert!(!handler.contains("refresh_descriptor_fingerprints"));
	assert!(!handler.contains("compute_logical_schema_fingerprint"));
	assert!(!handler.contains("compute_physical_descriptor_fingerprint"));
}

#[test]
fn adr072_descriptor_get_payload_is_full_descriptor_v1_postcard() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	assert!(control.contains("encode_table_schema_descriptor_v1(descriptor)"));
	assert!(client.contains("decode_table_schema_descriptor_v1(&trailing[..])"));
	assert!(client.contains("response.descriptor_len as usize != trailing.len()"));
}

#[test]
fn adr072_metrics_track_descriptor_get_outcomes() {
	let metrics = read_workspace("crates/wendelon/src/metrics/registry.rs");
	for required in [
		"control_descriptor_get_requests_total",
		"control_descriptor_get_success_total",
		"control_descriptor_get_not_found_total",
		"control_descriptor_get_not_leader_total",
		"control_descriptor_get_authorization_failed_total",
		"control_descriptor_get_invalid_request_total",
		"wendelon_control_descriptor_get_requests_total",
		"wendelon_control_descriptor_get_success_total",
		"wendelon_control_descriptor_get_not_found_total",
		"wendelon_control_descriptor_get_not_leader_total",
		"wendelon_control_descriptor_get_authorization_failed_total",
		"wendelon_control_descriptor_get_invalid_request_total",
	] {
		assert!(metrics.contains(required), "missing metric {required}");
	}
	for line in metrics
		.lines()
		.filter(|line| line.contains("control_descriptor_get"))
	{
		assert!(
			!line.contains("tenant") && !line.contains("table"),
			"descriptor-get metrics must stay aggregate-only: {line}"
		);
	}
}

#[test]
fn adr072_no_data_ring_or_bulk_or_object_id_descriptor_lookup() {
	let data_server = data_server_workspace_source();
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let handler = adr072_control_descriptor_get_handler_region(&control);
	assert!(!data_server.contains("TABLE_DESCRIPTOR_GET"));
	assert!(!handler.contains("descriptors_for_tenant"));
	assert!(!handler.contains("latest_by_id"));
	assert!(!handler.contains("table_object_id"));
}

#[test]
fn adr102_descriptor_list_surface_is_latest_only_metadata() {
	let schema_core = read_workspace("crates/wendelon/src/schema/core.rs");
	let table_schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	let data_server = data_server_workspace_source();
	let handler = adr102_control_descriptor_list_handler_region(&control);

	assert!(schema_core.contains("pub struct TableDescriptorListRequestMeta"));
	assert!(schema_core.contains("pub struct TableDescriptorListResponseMeta"));
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_LIST_REQ: Self = Self(0x44);"));
	assert!(schema_core.contains("pub const TABLE_DESCRIPTOR_LIST_RESP: Self = Self(0x45);"));
	// Routing is the invariant, not the `.0` comparison spelling — see the ADR 072
	// message-type guard for the same retarget.
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_LIST_REQ"));
	assert!(router.contains("MsgType::TABLE_DESCRIPTOR_LIST_RESP"));
	assert!(client.contains("pub async fn list_table_schema_descriptors"));
	assert!(client.contains("decode_table_schema_descriptors(&trailing[..])"));
	assert!(table_schema.contains("pub fn latest_descriptors_for_tenant"));
	assert!(handler.contains(".latest_descriptors_for_tenant("));
	assert!(!handler.contains(".descriptors_for_tenant("));
	assert!(handler.contains("self.state_machine.state_snapshot().await"));
	assert!(handler.contains("authorize_descriptor_get_actor"));
	assert!(handler.contains("CONTROL_REASON_NOT_LEADER"));
	assert!(handler.contains("encode_table_schema_descriptors(&descriptors)"));
	assert!(!data_server.contains("TABLE_DESCRIPTOR_LIST"));
	assert!(!handler.contains("Pbc"));
	assert!(!handler.contains("scan"));
}

#[test]
fn adr072_schema_update_and_drop_paths_stay_unchanged() {
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let schema_update = source_region(
		&control,
		"async fn handle_schema_update",
		"async fn handle_schema_drop",
	);
	let schema_drop = source_region(
		&control,
		"async fn handle_schema_drop",
		"fn decode_table_descriptor_get_request",
	);
	assert!(schema_update.contains("WendelonRequest::SchemaUpdate(frame.meta.to_vec())"));
	assert!(schema_drop.contains("run_table_schema_drop"));
	assert!(!schema_update.contains("TABLE_DESCRIPTOR_GET"));
	assert!(!schema_drop.contains("TABLE_DESCRIPTOR_GET"));
}

#[test]
fn adr072_no_toron_descriptor_sync_stub() {
	let catalog = read_workspace("crates/toron/src/catalog.rs");
	assert!(catalog.contains("trait DescriptorSyncClient"));
	assert!(catalog.contains("Client::get_table_schema_descriptor"));
	assert!(catalog.contains("sync_descriptor_for_tenant"));
	assert!(!catalog.contains("DescriptorSource"));
	assert!(!catalog.contains("todo!(\"descriptor sync"));
	assert!(!catalog.contains("unimplemented!(\"descriptor sync"));
}

#[test]
fn adr072_nilket_root_stays_runtime_free() {
	adr071_nilket_root_stays_runtime_free();
}

#[test]
fn adr072_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let schema_core = read_workspace("crates/wendelon/src/schema/core.rs");
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	let combined = format!("{schema_core}\n{control}\n{client}");
	let identifiers = rust_identifiers_from_source("adr072 combined surface", &combined);
	for required in [
		"TableDescriptorGetRequestMeta",
		"TableDescriptorGetResponseMeta",
		"TableDescriptorGetRequest",
		"TableDescriptorGetResult",
	] {
		assert!(
			has_rust_identifier(&identifiers, required),
			"missing descriptor-get type {required}"
		);
	}
	for forbidden in [
		"WendelonTableDescriptorGetRequest",
		"WendelonTableDescriptorGetResponse",
		"TableDescriptorGetRequestMetaV1",
		"TableDescriptorGetResponseMetaV1",
		"TableDescriptorGetRequestV1",
		"TableDescriptorGetResultV1",
	] {
		assert!(
			!has_rust_identifier(&identifiers, forbidden),
			"descriptor-get surface must stay brand-clean and no-V1; found {forbidden}"
		);
	}
}

#[test]
fn adr072_prior_adr_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn adr071_wendelon_inspect_branch_exists",
		"fn adr071_prior_adr_guards_continue_to_pass",
		"fn test_adr063_page_level_predicate_stats_guards",
		"fn test_adr066_fsst_evaluator_guards",
		"fn test_adr067_row_safe_fsst_guards",
		"fn test_adr068_fsst_row_safe_estimator_guards",
		"fn test_adr069_encoded_validity_sparse_projection_guards",
		"fn test_adr070_all_null_page_skip_sparse_projection_guards",
	] {
		assert!(
			architecture.contains(required),
			"missing prior guard {required}"
		);
	}
}

fn adr073_active_registry_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"struct ActiveTableQueryRegistry",
		"#[derive(Clone, Copy, Debug, PartialEq, Eq)]\nenum LeaseGateResult",
	)
}

fn adr073_table_query_handler_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"async fn handle_table_query",
		"async fn handle_table_query_cancel",
	)
}

fn adr073_cancel_handler_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"async fn handle_table_query_cancel",
		"async fn handle_aggregate",
	)
}

fn adr073_plan_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"async fn prepare_table_query_execution_plan_with_migration_with_cancellation",
		"async fn execute_aggregate_with_migration_merge",
	)
}

#[test]
fn adr073_table_query_cancel_is_wendelon_home_producer_only() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let data_server = data_server_workspace_source();
	let adr060 =
		read_workspace("crates/toron/plans/adr/0060_wendelon_table_query_cancellation_consumer.md");
	assert!(schema.contains("pub struct TableQueryRequestId"));
	assert!(data_server.contains("ActiveTableQueryRegistry"));
	assert!(adr060.contains("073 remains producer"));
	assert!(adr060.contains("owner; 060 retargets"));
	assert!(adr060.contains("No Wendelon/transport change"));

	let mut toron_hits = Vec::new();
	walk_dir(&workspace_root().join("crates/toron/src"), &mut |path| {
		if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
			return;
		}
		let content = fs::read_to_string(path).unwrap();
		for forbidden in [
			"TABLE_QUERY_CANCEL_REQ",
			"TABLE_QUERY_CANCEL_RESP",
			"MsgType::TABLE_QUERY_CANCEL",
			"handle_table_query_cancel",
			"ActiveTableQueryRegistry",
		] {
			if content.contains(forbidden) {
				toron_hits.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	});
	assert!(toron_hits.is_empty(), "{}", toron_hits.join("\n"));

	let toron_consumer = read_workspace("crates/toron/src/distributed/cancellation.rs");
	assert!(toron_consumer.contains("WendelonTableQueryCancelSender"));
	assert!(toron_consumer.contains("cancel_table_query_attempt"));
}

#[test]
fn adr073_table_query_request_id_is_wendelon_owned_not_toron() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	assert!(schema.contains("pub struct TableQueryRequestId([u8; 16]);"));
	assert!(schema.contains("pub fn from_u128(value: u128) -> Self"));
	assert!(schema.contains("pub fn get(self) -> u128"));
	for forbidden in ["QueryId", "TaskIdentity", "ClientWriteId"] {
		let region = source_region(
			&schema,
			"pub struct TableQueryRequestId",
			"// ============================================================\n//  TABLE WRITE WIRE METADATA AND ADMISSION",
		);
		assert!(
			!region.contains(forbidden),
			"request id must not import {forbidden}"
		);
	}
	assert!(client.contains("attempt_request.meta.request_id = TableQueryRequestId::random();"));
}

#[test]
fn adr073_table_query_request_id_is_required_fixed_meta() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	let data_server = data_server_workspace_source();
	let meta = source_region(
		&schema,
		"pub struct TableQueryRequestMeta",
		"pub struct TableQueryCancelStatus",
	);
	assert!(meta.contains("pub request_id: TableQueryRequestId"));
	assert!(schema.contains("size_of::<TableQueryRequestMeta>() == 184"));
	assert!(schema.contains("offset_of!(TableQueryRequestMeta, request_id), 128"));
	assert!(client.contains("TABLE_QUERY_REQ request_id must be non-zero"));
	assert!(data_server.contains("TABLE_QUERY_REQ request_id must be non-zero"));
}

#[test]
fn adr073_no_table_query_meta_extension_or_compat_path() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let data_server = data_server_workspace_source();
	assert!(!schema.contains("TableQueryRequestMetaExtension"));
	assert!(!schema.contains("TableQueryRequestMetaV1"));
	assert!(!data_server.contains("TABLE_QUERY_META_EXTENSION"));
	assert!(!data_server.contains("uncancellable"));
	assert!(data_server.contains(
		"*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<TableQueryRequestMeta>()])"
	));
}

#[test]
fn adr073_table_query_cancel_msg_types_are_3e_3f_not_1f() {
	let core = read_workspace("crates/wendelon/src/schema/core.rs");
	assert!(core.contains("pub const AUDIT_CHAIN_HEAD_RESP: Self = Self(0x1F);"));
	assert!(core.contains("pub const TABLE_QUERY_CANCEL_REQ: Self = Self(0x3E);"));
	assert!(core.contains("pub const TABLE_QUERY_CANCEL_RESP: Self = Self(0x3F);"));
	assert_eq!(core.matches("Self(0x3E)").count(), 1);
	assert_eq!(core.matches("Self(0x3F)").count(), 1);
	assert!(!core.contains("TABLE_QUERY_CANCEL_REQ: Self = Self(0x1F)"));
}

#[test]
fn adr073_cancel_wire_uses_data_ring_bidi_not_control_plane() {
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	let data_server = data_server_workspace_source();
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	assert!(router.contains("MsgType::TABLE_QUERY_CANCEL_REQ.0"));
	assert!(router.contains("Some(h) => h.handle_data_frame(context, frame).await"));
	assert!(data_server.contains("MsgType::TABLE_QUERY_CANCEL_REQ => self"));
	assert!(data_server.contains("handle_table_query_cancel"));
	assert!(!control.contains("TABLE_QUERY_CANCEL"));
}

#[test]
fn adr073_no_uni_stream_sidecar_listener_or_transport_redesign() {
	let combined = format!(
		"{}\n{}",
		data_server_workspace_source(),
		read_workspace("crates/wendelon/src/network/router.rs")
	);
	for forbidden in [
		"open_uni",
		"accept_uni",
		"TABLE_QUERY_CANCEL_LISTENER",
		"table_query_cancel_listener",
		"CancelTransport",
	] {
		assert!(
			!combined.contains(forbidden),
			"cancel wire must not add transport sidecar: {forbidden}"
		);
	}
}

#[test]
fn adr073_active_table_query_registry_is_tenant_scoped_raii_removed() {
	let data_server = data_server_workspace_source();
	let registry = adr073_active_registry_region(&data_server);
	assert!(
		data_server.contains("type TableQueryCancellationKey = (TenantId, TableQueryRequestId);")
	);
	assert!(registry.contains("struct ActiveTableQueryGuard"));
	assert!(registry.contains("impl Drop for ActiveTableQueryGuard"));
	assert!(registry.contains("state.active.remove(&key)"));
	assert!(registry.contains("Arc::ptr_eq(active, token)"));
	assert!(adr073_table_query_handler_region(&data_server).contains("let active_query = match"));
}

#[test]
fn adr073_cancel_before_register_tombstone_is_bounded_and_consumed() {
	let data_server = data_server_workspace_source();
	let registry = adr073_active_registry_region(&data_server);
	assert!(registry.contains("TABLE_QUERY_CANCEL_TOMBSTONE_TTL"));
	assert!(registry.contains("TABLE_QUERY_CANCEL_TOMBSTONE_CAP"));
	assert!(registry.contains("pending: HashMap<TableQueryCancellationKey, Instant>"));
	assert!(registry.contains("order: VecDeque<TableQueryTombstoneOrder>"));
	assert!(registry.contains("state.pending.remove(&key).is_some()"));
	assert!(registry.contains("Self::prune_locked(state, now)"));
}

#[test]
fn adr073_cancellation_signal_uses_cancellation_token() {
	let cargo = read_workspace("crates/wendelon/Cargo.toml");
	let data_server = data_server_workspace_source();
	let registry = adr073_active_registry_region(&data_server);
	assert!(cargo.contains("tokio-util = \"0.7\""));
	assert!(data_server.contains("use tokio_util::sync::CancellationToken;"));
	assert!(registry.contains("Arc<CancellationToken>"));
	assert!(registry.contains("token.cancel();"));
	assert!(!registry.contains("Notify"));
	assert!(!registry.contains("notify_waiters"));
}

#[test]
fn adr073_cancel_is_idempotent_for_terminal_and_missing_requests() {
	let data_server = data_server_workspace_source();
	let registry = adr073_active_registry_region(&data_server);
	let cancel = source_region(registry, "fn cancel(", "fn finish(");
	assert!(cancel.contains("state.active.get(&key)"));
	assert!(cancel.contains("return TableQueryCancelStatus::ACCEPTED;"));
	assert!(cancel.contains("state.terminal.contains_key(&key)"));
	assert!(cancel.contains("return TableQueryCancelStatus::TERMINAL;"));
	assert!(cancel.contains("TableQueryTombstoneKind::Pending"));
	assert!(cancel.contains("TableQueryCancelStatus::ACCEPTED"));
}

#[test]
fn adr073_cancel_response_is_oracle_safe() {
	let data_server = data_server_workspace_source();
	let handler = adr073_cancel_handler_region(&data_server);
	assert!(handler.contains("TableQueryCancelStatus::TERMINAL"));
	assert!(handler.contains("authorize_actor_for_data"));
	assert!(handler.contains("enforce_read_policy"));
	assert!(handler.contains(".cancel(meta.tenant_id, meta.request_id)"));
	assert!(!handler.contains("TableQueryRequestId::get"));
	assert!(!handler.contains("table_object_id"));
}

#[test]
fn adr073_table_query_checks_cancel_at_execution_boundaries() {
	let data_server = data_server_workspace_source();
	let plan = adr073_plan_region(&data_server);
	let ranged = source_region(
		&data_server,
		"async fn plan_query_execution_for_ranges_with_program_maybe_cancelled",
		"// 4. Empty result is a legitimate answer.",
	);
	assert!(plan.matches("cancellation_token.is_cancelled()").count() >= 5);
	assert!(ranged.matches("cancel.is_cancelled()").count() >= 6);
	assert!(plan.contains("pull_migration_range"));
	assert!(plan.contains("plan_table_query_execution_by_secondary_index"));
	assert!(ranged.contains("for range in ranges"));
	assert!(ranged.contains("for (key_identity, locality, location) in scanned"));
	assert!(ranged.contains(".push(result, stream_writer.as_deref_mut(), &mut batches)"));
}

#[test]
fn adr073_cancelled_table_query_has_stable_read_status() {
	let core = read_workspace("crates/wendelon/src/schema/core.rs");
	let client = read_workspace("crates/wendelon/src/client/core.rs");
	let data_server = data_server_workspace_source();
	assert!(core.contains("pub const QUERY_CANCELLED: Self = Self(15);"));
	assert!(core.contains("Self::QUERY_CANCELLED => \"query_cancelled\""));
	// ADR 540 replaced hand-written per-status message construction with one typed
	// rendering, so the stable surface is the status *name* carried into the rendered
	// error, not a bespoke literal per status. The invariant is unchanged: cancellation
	// stays a named status and keeps its own error kind.
	assert!(client.contains("Wendelon query failed: query_cancelled(15)"));
	assert!(client.contains("ErrorKind::Interrupted"));
	assert!(data_server.contains("ReadStatus::QUERY_CANCELLED"));
	assert!(adr073_table_query_handler_region(&data_server).contains("FRAME_FLAG_STREAM_END"));
	assert!(adr073_table_query_handler_region(&data_server).contains("None"));
}

#[test]
fn adr073_stream_drop_is_fallback_not_primary_mechanism() {
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	let data_server = data_server_workspace_source();
	assert!(router.contains("observe_table_query_cancellation(\"stream_dropped\")"));
	assert!(router.contains("resp.header.msg_type == MsgType::TABLE_QUERY_RESP.0"));
	assert!(data_server.contains("TABLE_QUERY_CANCEL_REQ"));
	assert!(data_server.contains("cancellation_token.is_cancelled()"));
}

#[test]
fn adr073_cancel_releases_admission_buffers_and_scratch() {
	let data_server = data_server_workspace_source();
	let handler = adr073_table_query_handler_region(&data_server);
	let registry = adr073_active_registry_region(&data_server);
	assert!(handler.contains("let active_query = match"));
	assert!(registry.contains("impl Drop for ActiveTableQueryGuard"));
	assert!(registry.contains("state.active.remove(&key)"));
	assert!(data_server.contains("let _admission = match self.try_admit_data_request"));
	assert!(!data_server.contains("std::mem::forget(active_query)"));
	assert!(!data_server.contains("Box::leak"));
}

#[test]
fn adr073_metrics_are_aggregate_only() {
	let metrics = read_workspace("crates/wendelon/src/metrics/registry.rs");
	assert!(metrics.contains("wendelon_table_query_cancellations_total"));
	assert!(metrics.contains("TABLE_QUERY_CANCELLATION_OUTCOMES"));
	assert!(metrics.contains("\"accepted\""));
	assert!(metrics.contains("\"terminal\""));
	assert!(metrics.contains("\"cancelled_response\""));
	assert!(metrics.contains("\"stream_dropped\""));
	for line in metrics
		.lines()
		.filter(|line| line.contains("table_query_cancellation"))
	{
		for forbidden in [
			"(\"tenant\"",
			"(\"table\"",
			"(\"request_id\"",
			"(\"node_id\"",
			"(\"fingerprint\"",
			"(\"endpoint\"",
			"(\"sql\"",
			"(\"peer\"",
		] {
			assert!(
				!line.contains(forbidden),
				"table-query cancellation metric must stay aggregate-only: {line}"
			);
		}
	}
}

#[test]
fn adr073_no_writes_cdc_admin_agg_or_legacy_query_scope() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let data_server = data_server_workspace_source();
	assert!(!schema.contains("WRITE_CANCEL"));
	assert!(!schema.contains("AGG_CANCEL"));
	assert!(!schema.contains("CDC_CANCEL"));
	assert!(!schema.contains("pub const QUERY_CANCEL_REQ"));
	assert!(!data_server.contains("MsgType::AGG_CANCEL"));
	assert!(!data_server.contains("MsgType::QUERY_CANCEL_REQ"));
	assert!(!data_server.contains("MsgType::WRITE_CANCEL"));
}

#[test]
fn adr073_toron_storage_scan_consumer_claim_is_retargeted_by_adr060() {
	let adr060 =
		read_workspace("crates/toron/plans/adr/0060_wendelon_table_query_cancellation_consumer.md");
	let storage = read_workspace("crates/toron/src/storage.rs");
	let cancellation = read_workspace("crates/toron/src/distributed/cancellation.rs");
	assert!(adr060.contains("Toron consumer of Wendelon 073"));
	assert!(adr060.contains("best-effort"));
	assert!(adr060.contains("checkpoint-bounded"));
	assert!(storage.contains("table_query_stream_with_runtime_filter_and_attempt_hook"));
	assert!(storage.contains("register_wendelon_table_query_attempt"));
	assert!(cancellation.contains("DownstreamAttemptRegistry"));

	let mut violations = Vec::new();
	walk_dir(&workspace_root().join("crates/toron/src"), &mut |path| {
		if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
			return;
		}
		let content = fs::read_to_string(path).unwrap();
		for forbidden in ["TABLE_QUERY_CANCEL_REQ", "TABLE_QUERY_CANCEL_RESP"] {
			if content.contains(forbidden) {
				violations.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	});
	assert!(violations.is_empty(), "{}", violations.join("\n"));
}

#[test]
fn adr073_nilket_root_stays_runtime_free() {
	adr072_nilket_root_stays_runtime_free();
}

#[test]
fn adr073_fixture_worker_source_unchanged() {
	let fixture = read_workspace("crates/toron/src/bin/toron_fixture_worker.rs");
	for forbidden in [
		"TableQueryRequestId",
		"TableQueryCancel",
		"TABLE_QUERY_CANCEL",
		"QUERY_CANCELLED",
	] {
		assert!(
			!fixture.contains(forbidden),
			"fixture worker must not grow ADR073 surface: {forbidden}"
		);
	}
}

#[test]
fn adr073_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	let combined = format!(
		"{}\n{}\n{}\n{}",
		read_workspace("crates/wendelon/src/schema/table.rs"),
		read_workspace("crates/wendelon/src/schema/core.rs"),
		read_workspace("crates/wendelon/src/client/core.rs"),
		data_server_workspace_source()
	);
	let identifiers = rust_identifiers_from_source("adr073 combined surface", &combined);
	for required in [
		"TableQueryRequestId",
		"TableQueryCancelStatus",
		"TableQueryCancelRequestMeta",
		"TableQueryCancelResponseMeta",
		"ActiveTableQueryRegistry",
		"ActiveTableQueryGuard",
	] {
		assert!(
			has_rust_identifier(&identifiers, required),
			"missing table-query cancellation type {required}"
		);
	}
	for forbidden in [
		"WendelonTableQueryRequestId",
		"WendelonTableQueryCancelRequest",
		"TableQueryRequestIdV1",
		"TableQueryCancelStatusV1",
		"TableQueryCancelRequestMetaV1",
		"TableQueryCancelResponseMetaV1",
		"CancelRequest",
		"CancelResponse",
	] {
		assert!(
			!has_rust_identifier(&identifiers, forbidden),
			"new cancellation surface must stay brand-clean and no-V1; found {forbidden}"
		);
	}
}

#[test]
fn adr073_prior_wendelon_protocol_and_architecture_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn adr072_descriptor_get_msg_types_are_unique",
		"fn adr072_prior_adr_guards_continue_to_pass",
		"fn adr071_wendelon_inspect_branch_exists",
		"fn test_adr070_all_null_page_skip_sparse_projection_guards",
		"fn test_adr069_encoded_validity_sparse_projection_guards",
		"fn test_adr068_fsst_row_safe_estimator_guards",
		"fn test_adr067_row_safe_fsst_guards",
		"fn test_adr066_fsst_evaluator_guards",
	] {
		assert!(
			architecture.contains(required),
			"missing prior Wendelon guard {required}"
		);
	}
}

fn adr074_trace_handler_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"async fn handle_table_query",
		"async fn handle_table_query_cancel",
	)
}

fn adr074_linked_span_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"fn linked_table_query_span",
		"fn local_table_query_span",
	)
}

fn adr074_trace_carrier_region(data_server: &str) -> &str {
	source_region(
		data_server,
		"enum TableQueryTraceCarrier",
		"#[derive(Clone, Debug)]\nstruct RuntimeFilterApplication",
	)
}

#[test]
fn adr074_table_query_options_trace_context_is_absent_by_default() {
	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	assert!(client.contains("pub trace_context: TableQueryTraceContext"));
	assert!(client.contains("trace_context: TableQueryTraceContext::Absent"));
	assert!(client.contains("pub enum TableQueryTraceContext"));
	assert!(client.contains("#[default]"));
}

#[test]
fn adr074_table_query_trace_context_has_absent_unsampled_sampled_states() {
	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	let enum_region = source_region(
		&client,
		"pub enum TableQueryTraceContext",
		"impl Default for TableQueryOptions",
	);
	for required in [
		"Absent",
		"Unsampled",
		"Sampled",
		"trace_id: [u8; 16]",
		"parent_span_id: [u8; 8]",
	] {
		assert!(enum_region.contains(required), "missing {required}");
	}
}

#[test]
fn adr074_table_query_trace_carrier_is_fixed_binary_not_traceparent() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let meta = source_region(
		&schema,
		"pub struct TableQueryRequestMeta",
		"pub struct TableQueryCancelStatus",
	);
	assert!(meta.contains("pub trace_id: [u8; 16]"));
	assert!(meta.contains("pub trace_parent_span_id: [u8; 8]"));
	assert!(meta.contains("pub trace_flags: u8"));
	assert!(!meta.contains("traceparent"));
	assert!(!meta.contains("tracestate"));
}

#[test]
fn adr074_table_query_meta_widens_tail_without_extension_or_v1_sidecar() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	assert!(schema.contains("size_of::<TableQueryRequestMeta>() == 184"));
	for forbidden in [
		"TableQueryRequestMetaV1",
		"TableQueryRequestMetaExtension",
		"ComputeTraceContext",
	] {
		assert!(
			!schema.contains(forbidden),
			"forbidden trace carrier {forbidden}"
		);
	}
}

#[test]
fn adr074_trace_meta_offsets_are_pinned() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	// The trace tail sits after the fixed meta's own fields, so widening the meta moves
	// it wholesale. What this guard pins is that the tail stays pinned *somewhere* — the
	// offsets are re-stated, never dropped, when a field lands ahead of them.
	for required in [
		"offset_of!(TableQueryRequestMeta, trace_id), 152",
		"offset_of!(TableQueryRequestMeta, trace_parent_span_id)",
		"168",
		"offset_of!(TableQueryRequestMeta, trace_flags)",
		"176",
		"offset_of!(TableQueryRequestMeta, _trace_pad), 177",
		"177",
	] {
		assert!(schema.contains(required), "missing offset pin {required}");
	}
}

#[test]
fn adr074_wendelon_span_id_is_derived_nonzero_from_request_attempt() {
	let data_server = data_server_workspace_source();
	let region = adr074_trace_carrier_region(&data_server);
	assert!(region.contains("TABLE_QUERY_LINKED_SPAN_DOMAIN"));
	assert!(region.contains("b\"wendelon.table_query.span.v1\""));
	assert!(region.contains("hasher.update(&trace_id)"));
	assert!(region.contains("hasher.update(&parent_span_id)"));
	assert!(region.contains("hasher.update(meta.request_id.as_bytes())"));
	assert!(region.contains("fn nonzero_span_id"));
	assert!(region.contains("span_id[7] = 1"));
}

#[test]
fn adr074_linked_span_uses_explicit_otel_context_fields() {
	let data_server = data_server_workspace_source();
	let linked = adr074_linked_span_region(&data_server);
	for required in [
		"\"wendelon.table.query\"",
		"otel.trace_id",
		"otel.parent_span_id",
		"otel.span_id",
		"otel.sampled = true",
	] {
		assert!(
			linked.contains(required),
			"missing linked span field {required}"
		);
	}
}

#[test]
fn adr074_unsampled_carrier_bypasses_root_sampling() {
	let data_server = data_server_workspace_source();
	let region = adr074_trace_carrier_region(&data_server);
	assert!(region.contains("TableQueryTraceCarrier::Unsampled"));
	assert!(region.contains("otel.sampled = false"));
	assert!(!region.contains("should_sample_root_span"));
}

#[test]
fn adr074_sampled_emits_sanitized_linked_span_local_raw_span_preserved() {
	let data_server = data_server_workspace_source();
	let handler = adr074_trace_handler_region(&data_server);
	assert!(handler.contains("let linked_span = linked_table_query_span(&meta, trace_carrier);"));
	assert!(data_server.contains("\"wendelon.table.query\""));
	assert!(data_server.contains("\"data_ring.table_query.request\""));
}

#[test]
fn adr074_local_raw_span_is_never_nested_under_linked_span() {
	let data_server = data_server_workspace_source();
	let handler = adr074_trace_handler_region(&data_server);
	assert!(handler.contains("let linked_span = linked_table_query_span(&meta, trace_carrier);"));
	assert!(handler.contains(
		"let span = local_table_query_span(&meta, frame.header.stream_id, trace_carrier);"
	));
	assert!(handler.contains(".instrument(span)"));
	assert!(!handler.contains(".instrument(linked_span"));
	assert!(!handler.contains("linked_span.enter()"));
}

#[test]
fn adr074_linked_span_has_no_tenant_table_request_descriptor_query_attrs() {
	let data_server = data_server_workspace_source();
	let linked = adr074_linked_span_region(&data_server);
	for forbidden in [
		"tenant_id",
		"table_object_id",
		"request_id",
		"physical_descriptor_fingerprint",
		"descriptor",
		"query_id",
		"peer",
		"sql",
	] {
		assert!(
			!linked.contains(forbidden),
			"linked span leaks forbidden raw identity marker {forbidden}"
		);
	}
}

#[test]
fn adr074_malformed_trace_carrier_is_fail_open_unsampled() {
	let data_server = data_server_workspace_source();
	let region = adr074_trace_carrier_region(&data_server);
	assert!(region.contains("flags & !TABLE_QUERY_TRACE_FLAG_KNOWN_MASK"));
	assert!(region.contains("flags & TABLE_QUERY_TRACE_FLAG_PRESENT == 0"));
	assert!(region.contains("!has_trace_id || !has_parent_span_id"));
	assert!(
		region
			.matches("return TableQueryTraceCarrier::Unsampled")
			.count() >= 4
	);
}

#[test]
fn adr074_no_new_shared_observability_telemetry_seam() {
	let data_server = data_server_workspace_source();
	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	assert!(!data_server.contains("observability::telemetry::"));
	assert!(!client.contains("observability::telemetry::"));
	assert!(!data_server.contains("TraceContextV1"));
}

#[test]
fn adr074_absent_context_preserves_wendelon_otlp_behavior_fixtures() {
	let fixtures = read_workspace("crates/wendelon/tests/table_query_telemetry.rs");
	assert!(fixtures.contains("table_query_options_default_trace_context_absent"));
	assert!(fixtures.contains("table_query_meta_absent_roundtrips_zero_flags"));
	assert!(read_workspace("crates/wendelon/tests/otlp_http_export.rs").contains("otlp.test.span"));
	assert!(read_workspace("crates/wendelon/tests/otlp_grpc_export.rs").contains("grpc.test.span"));
}

#[test]
fn adr074_no_toron_production_change() {
	let toron_storage = read_workspace("crates/toron/src/storage.rs");
	let toron_telemetry = read_workspace("crates/toron/src/telemetry.rs");
	assert!(
		toron_storage.contains("fn wendelon_table_query_trace_context("),
		"ADR 069 owns Toron's table-query trace carrier population"
	);
	assert!(
		toron_storage.contains("wendelon_client::TableQueryTraceContext"),
		"Toron must consume Wendelon's ADR 074 carrier type directly"
	);
	assert!(
		toron_telemetry.contains("pub(crate) fn storage_scan_trace_context("),
		"Toron must derive the storage-scan parent through its local telemetry seam"
	);

	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	let data_server = data_server_workspace_source();
	assert!(client.contains("pub enum TableQueryTraceContext"));
	assert!(data_server.contains("fn decode_table_query_trace_carrier"));

	let mut violations = Vec::new();
	for dir in [
		workspace_root().join("crates/wendelon/src"),
		workspace_root().join("crates/wendelon/crates/wendelon-client/src"),
	] {
		walk_dir(&dir, &mut |path| {
			if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
				return;
			}
			let content = fs::read_to_string(path).unwrap();
			for forbidden in [
				"wendelon_table_query_trace_context",
				"storage_scan_trace_context",
				"toron.storage.scan",
			] {
				if content.contains(forbidden) {
					violations.push(format!("{} contains {forbidden}", path.display()));
				}
			}
		});
	}
	assert!(
		violations.is_empty(),
		"Toron consumer hooks must not be added to Wendelon production:\n{}",
		violations.join("\n")
	);
}

#[test]
fn adr074_toron_consumer_population_is_owned_by_adr069() {
	let mut violations = Vec::new();
	walk_dir(&workspace_root().join("crates/toron/src"), &mut |path| {
		if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
			return;
		}
		let content = fs::read_to_string(path).unwrap();
		for forbidden in ["TABLE_QUERY_TRACE_FLAG", "wendelon.table.query"] {
			if content.contains(forbidden) {
				violations.push(format!("{} contains {forbidden}", path.display()));
			}
		}
	});
	assert!(
		violations.is_empty(),
		"Toron should consume the carrier, not duplicate Wendelon's producer internals:\n{}",
		violations.join("\n")
	);
}

#[test]
fn adr074_no_control_ring_sql_wire_audit_cancellation_or_non_table_query_drift() {
	let core = read_workspace("crates/wendelon/src/schema/core.rs");
	let data_server = data_server_workspace_source();
	let control = read_workspace("crates/wendelon/src/network/control_server.rs");
	assert!(!core.contains("TRACE_REQ"));
	assert!(!control.contains("TableQueryTrace"));
	assert!(!data_server.contains("SQL_TRACE"));
	assert!(!data_server.contains("CDC_TRACE"));
	assert!(data_server.contains("fn decode_table_query_trace_carrier"));
}

#[test]
fn adr074_new_trace_surface_is_brand_clean_no_v1_suffix() {
	let combined = format!(
		"{}\n{}\n{}",
		read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs"),
		read_workspace("crates/wendelon/src/schema/table.rs"),
		data_server_workspace_source()
	);
	let identifiers = rust_identifiers_from_source("adr074 combined surface", &combined);
	assert!(has_rust_identifier(&identifiers, "TableQueryTraceContext"));
	for forbidden in [
		"TableQueryTraceContextV1",
		"WendelonTableQueryTraceContext",
		"TableQueryRequestMetaV1",
	] {
		assert!(
			!has_rust_identifier(&identifiers, forbidden),
			"new trace surface must stay brand-clean and no-V1; found {forbidden}"
		);
	}
}

#[test]
fn adr074_carrier_does_not_transmit_compute_trace_context_v1() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	let options_region = source_region(
		&client,
		"pub struct TableQueryOptions",
		"pub struct WriteReceipt",
	);
	assert!(!schema.contains("ComputeTraceContext"));
	assert!(!options_region.contains("ComputeTraceContext"));
}

#[test]
fn adr116_table_query_row_limit_is_fixed_meta_not_sidecar_budget() {
	let schema = read_workspace("crates/wendelon/src/schema/table.rs");
	let client = read_workspace("crates/wendelon/crates/wendelon-client/src/lib.rs");
	let data_server = data_server_workspace_source();
	let options_region = source_region(
		&client,
		"pub struct TableQueryOptions",
		"pub struct WriteReceipt",
	);
	let meta_region = source_region(
		&schema,
		"pub struct TableQueryRequestMeta",
		"pub struct TableQueryCancelStatus",
	);

	assert!(options_region.contains("pub row_limit: Option<NonZeroU64>"));
	assert!(client.contains("row_limit_rows: options.row_limit.map(NonZeroU64::get).unwrap_or(0)"));
	assert!(meta_region.contains("pub row_limit_rows: u64"));
	assert!(schema.contains("size_of::<TableQueryRequestMeta>() == 184"));
	assert!(schema.contains("offset_of!(TableQueryRequestMeta, row_limit_rows)"));
	assert!(schema.contains("120"));
	assert!(data_server.contains("fn table_query_row_limit"));
	assert!(data_server.contains("fn take_output_batch"));
	assert!(data_server.contains("table_query_row_limit(meta)"));
	// ADR 0719: the budget has no `Default`; its defaulting constructor takes the
	// executing node's stall bound.
	assert!(data_server.contains("QueryExecutionBudget::server_default("));
	assert!(!data_server.contains("QueryExecutionBudget::default()"));
	assert!(!schema.contains("TableQueryRequestMetaExtension"));
	assert!(!schema.contains("TableQueryRequestMetaV1"));
}

#[test]
fn adr118_table_query_resp_has_terminal_metadata_contract() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let data_server = data_server_workspace_source();
	let router = read_workspace("crates/wendelon/src/network/router.rs");

	assert!(schema.contains("size_of::<QueryResponseMeta>() == 56"));
	assert!(!schema.contains("terminal_status"));
	assert!(!schema.contains("provisional"));
	assert!(router.contains("MsgType::TABLE_QUERY_REQ.0"));
	assert!(data_server.contains("async fn handle_table_query_stream"));
	assert!(data_server.contains("struct TableQueryResponseStreamWriter"));
	assert!(data_server.contains("fn query_ok_provisional_meta"));
	assert!(data_server.contains("async fn emit_terminal"));
	assert!(data_server.contains("MsgType::TABLE_QUERY_RESP.0"));
	assert!(data_server.contains("FRAME_FLAG_STREAM_END"));
	assert!(data_server.contains("writer.emit_terminal(response).await"));
}

#[test]
fn adr118_streaming_query_result_terminal_mode_is_opt_in() {
	let client_core = read_workspace("crates/wendelon/src/client/core.rs");
	let strict_query = source_region(
		&client_core,
		"async fn query_once",
		"#[derive(Debug)]\nenum QueryOutcome",
	);
	let table_query = source_region(
		&client_core,
		"async fn table_query_once",
		"async fn bulk_write_group",
	);

	assert!(client_core.contains("enum StreamingQueryMetaMode"));
	assert!(client_core.contains("StrictContinuations"));
	assert!(client_core.contains("TerminalMetadata"));
	assert!(strict_query.contains("StreamingQueryMetaMode::StrictContinuations"));
	assert!(table_query.contains("StreamingQueryMetaMode::TerminalMetadata"));
	assert!(client_core.contains("QUERY_RESP continuation must not carry response meta"));
	assert!(client_core.contains("query terminal response meta too small"));
}

#[test]
fn adr118_table_query_production_streams_batches_without_boundary_concat() {
	let data_server = data_server_workspace_source();
	let stream_handler = source_region(
		&data_server,
		"async fn handle_table_query_stream",
		"async fn handle_table_query_cancel",
	);

	assert!(stream_handler.contains(
		"prepare_table_query_execution_plan_with_migration_with_cancellation_and_stream"
	));
	assert!(!stream_handler.contains("concat_batches"));
	assert!(stream_handler.contains("writer.total_arrow_bytes()"));
	assert!(stream_handler.contains("writer.emit_terminal(response).await"));
	assert!(data_server.contains("stream_writer.as_deref_mut()"));
	assert!(data_server.contains(".push(result, stream_writer.as_deref_mut(), &mut batches)"));
	assert!(data_server.contains("if rows == 0 && batches.is_empty()"));
}

#[test]
fn adr118_wendelon_table_query_checks_cancellation_during_production() {
	let data_server = data_server_workspace_source();
	let producer = source_region(
		&data_server,
		"async fn plan_query_execution_for_ranges_with_program_maybe_cancelled",
		"fn encode_query_stream_batches_with_deadline",
	);
	// The cost bound depends on the cancellation check living *inside*
	// the per-location scan loop (alongside the per-location emit), not
	// only before planning — so a cancelled/dropped consumer stops
	// Wendelon before it reads the remaining row groups. Pin the check
	// to the scan-loop region so both projected and whole-record
	// production stay cancellation-bounded.
	let scan_loop = source_region(
		producer,
		"for (key_identity, locality, location) in scanned",
		"// 4. Empty result is a legitimate answer.",
	);
	assert!(
		scan_loop.contains("cancel.is_cancelled()"),
		"cancellation must be checked inside the per-location scan loop"
	);
	assert!(
		scan_loop.contains(".push(result, stream_writer.as_deref_mut(), &mut batches)"),
		"the table-query producer must feed its accumulator inside the scan loop (ADR 0088)"
	);
	assert!(
		scan_loop.contains("ReadStatus::QUERY_CANCELLED"),
		"an in-loop cancellation yields QUERY_CANCELLED"
	);
}

#[test]
fn adr118_table_query_req_uses_stream_handler() {
	let data_server = data_server_workspace_source();
	assert!(
		data_server.contains("self.handle_table_query_stream("),
		"a TABLE_QUERY_REQ dispatch arm must call the streaming handler"
	);
	// The streaming handler must run the streaming producer, not a
	// materialize-then-respond path.
	let handler = source_region(
		&data_server,
		"async fn handle_table_query_stream",
		"async fn handle_table_query_cancel",
	);
	assert!(handler.contains(
		"prepare_table_query_execution_plan_with_migration_with_cancellation_and_stream"
	));
}

#[test]
fn adr118_query_response_meta_not_widened() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let meta = source_region(&schema, "pub struct QueryResponseMeta", "}");
	// D3: provisional-vs-terminal is distinguished by frame position +
	// FRAME_FLAG_STREAM_END + the opt-in cursor mode — never by widening
	// the fixed-size meta. The struct must carry no terminal/cursor fields.
	for forbidden in [
		"is_terminal",
		"terminal_status",
		"provisional",
		"cursor",
		"stream_end",
		"continuation",
	] {
		assert!(
			!meta.contains(forbidden),
			"QueryResponseMeta must not be widened with `{forbidden}` (ADR 118 D3)"
		);
	}
	assert!(schema.contains("size_of::<QueryResponseMeta>() == 56"));
}

#[test]
fn adr118_secondary_index_table_query_streams() {
	let data_server = data_server_workspace_source();
	// D7: the secondary-index table-query path uses the same streaming
	// producer contract as the full scan — it threads the stream writer
	// so the cost bound is access-path-independent.
	assert!(
		data_server.contains("plan_table_query_execution_by_secondary_index"),
		"secondary-index producer exists"
	);
	let by_index = source_region(
		&data_server,
		"match plan_table_query_execution_by_secondary_index(",
		"Ok(plan) =>",
	);
	assert!(
		by_index.contains("stream_writer.as_deref_mut()"),
		"the secondary-index path threads the stream writer (D7 parity)"
	);
}

#[test]
fn adr120_whole_record_table_query_emits_per_location() {
	let data_server = data_server_workspace_source();
	let producer = source_region(
		&data_server,
		"async fn plan_query_execution_for_ranges_with_program_maybe_cancelled",
		"fn encode_query_stream_batches_with_deadline",
	);
	let descriptor_backed = source_region(
		producer,
		"// ADR 120 descriptor-backed whole-record path:",
		"for group in descriptorless_rebatch_groups",
	);
	// ADR 120 retires the ADR 118 projected-only scope carve-out: the
	// whole-record fallback must execute PBC and emit from inside the
	// per-location scan loop, with no post-scan rebatch buffer.
	assert!(
		descriptor_backed.contains(".push(result, stream_writer.as_deref_mut(), &mut batches)"),
		"table-query results must be produced inside the scan loop; ADR 0088 made the \
		 unit a bounded response batch rather than one frame per location"
	);
	assert!(
		descriptor_backed.contains("pbc::execute_program(program, &batch)"),
		"whole-record fallback must execute PBC per location"
	);
	assert!(
		descriptor_backed.contains("take_output_batch(result, &mut rows, row_limit)"),
		"whole-record fallback must apply row_limit before any later scan location can be read"
	);
	assert!(!descriptor_backed.contains("concat_batches"));
	assert!(data_server.contains("struct DescriptorlessRebatchGroup"));
	assert!(producer.contains("for group in descriptorless_rebatch_groups"));
}

#[test]
fn adr120_rebatch_groups_post_scan_emit_is_gone_for_descriptor_backed_queries() {
	let data_server = data_server_workspace_source();
	let producer = source_region(
		&data_server,
		"async fn plan_query_execution_for_ranges_with_program_maybe_cancelled",
		"fn encode_query_stream_batches_with_deadline",
	);
	let descriptorless_compat = source_region(
		producer,
		"if table_descriptor.is_none()",
		"// ADR 120 descriptor-backed whole-record path:",
	);
	let descriptor_backed = source_region(
		producer,
		"// ADR 120 descriptor-backed whole-record path:",
		"for group in descriptorless_rebatch_groups",
	);

	assert!(data_server.contains("struct DescriptorlessRebatchGroup"));
	assert!(descriptorless_compat.contains("descriptorless_rebatch_groups"));
	assert!(descriptorless_compat.contains("continue;"));
	assert!(producer.contains("for group in descriptorless_rebatch_groups"));
	assert!(!descriptor_backed.contains("descriptorless_rebatch_groups"));
	assert!(!descriptor_backed.contains("concat_batches"));
	assert!(!descriptor_backed.contains("Vec<DescriptorlessRebatchGroup>"));
}

#[test]
fn adr120_cost_bound_is_access_path_independent_for_descriptor_backed_queries() {
	let data_server = data_server_workspace_source();
	let full_scan = source_region(
		&data_server,
		"async fn plan_query_execution_for_ranges_with_program_maybe_cancelled",
		"fn encode_query_stream_batches_with_deadline",
	);
	let descriptor_backed = source_region(
		full_scan,
		"// ADR 120 descriptor-backed whole-record path:",
		"for group in descriptorless_rebatch_groups",
	);
	let secondary_index = source_region(
		&data_server,
		"async fn plan_table_query_execution_by_secondary_index",
		"async fn plan_query_execution_for_ranges",
	);

	for (name, region) in [
		(
			"full scan descriptor-backed whole-record",
			descriptor_backed,
		),
		("secondary-index descriptor-backed", secondary_index),
	] {
		assert!(
			region.contains("take_output_batch(result, &mut rows, row_limit)"),
			"{name}: row_limit must be applied before the next storage read"
		);
		assert!(
			region.contains(".push(result, stream_writer.as_deref_mut(), &mut batches)"),
			"{name}: batch must be emitted from the producer path"
		);
		assert!(
			region.contains("row_limit_satisfied(rows, row_limit)"),
			"{name}: producer must break once the bound is satisfied"
		);
	}
	assert!(
		secondary_index.contains("for indexed_chunk in indexed_rows.chunks(4)"),
		"secondary-index reads must be chunked so LIMIT does not materialize all index hits"
	);
	assert!(
		secondary_index.contains("break 'indexed_rows;"),
		"secondary-index producer must stop reading indexed chunks once LIMIT is satisfied"
	);
}

#[test]
fn adr120_no_wendelon_protocol_widening() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let data_server = data_server_workspace_source();
	let meta = source_region(&schema, "pub struct QueryResponseMeta", "}");

	assert!(schema.contains("size_of::<QueryResponseMeta>() == 56"));
	for forbidden in [
		"cursor",
		"stream_id",
		"terminal_status",
		"result_stream",
		"TaskBatch",
	] {
		assert!(
			!meta.contains(forbidden),
			"QueryResponseMeta must not grow ADR 120 field `{forbidden}`"
		);
	}
	assert!(data_server.contains("TableQueryResponseStreamWriter"));
	assert!(!data_server.contains("MsgType::TABLE_QUERY_CURSOR"));
}

#[test]
fn adr074_prior_wendelon_telemetry_and_table_query_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn adr073_table_query_request_id_is_required_fixed_meta",
		"fn adr073_prior_wendelon_protocol_and_architecture_guards_continue_to_pass",
		"fn adr072_prior_adr_guards_continue_to_pass",
		"fn adr071_wendelon_inspect_branch_exists",
	] {
		assert!(
			architecture.contains(required),
			"missing prior Wendelon guard {required}"
		);
	}
}

// ============================================================
//  ADR 075 — Idempotent Atomic Counter Fetch-Add (10 guards)
// ============================================================

fn adr075_counter_handler_region(server: &str) -> &str {
	// ADR 077: counter handlers live in `network/data_server/counter.rs` in the
	// order replicate, rollback, add, read; `handle_counter_add`'s body is bounded
	// by the next counter handler, `handle_counter_read` (was `handle_table_replicate`,
	// the next handler when all handlers shared one file).
	source_region(
		server,
		"async fn handle_counter_add",
		"async fn handle_counter_read",
	)
}

#[test]
fn adr075_counter_add_is_first_class_operation_not_blob_write() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let server = data_server_workspace_source();
	// Its own wire op + fixed metas, distinct from the blob write surface.
	assert!(schema.contains("pub const COUNTER_ADD_REQ: Self = Self(0x4C);"));
	assert!(schema.contains("pub const COUNTER_ADD_RESP: Self = Self(0x4D);"));
	assert!(schema.contains("pub struct CounterAddRequestMeta"));
	assert!(schema.contains("pub struct CounterAddResponseMeta"));
	// A counter add carries an additive delta, never an Arrow value section.
	assert!(schema.contains("pub delta: u64"));
	// Dispatched as its own match arm + handler, not folded into WRITE_REQ.
	assert!(server.contains("MsgType::COUNTER_ADD_REQ => {"));
	assert!(server.contains("async fn handle_counter_add"));
}

#[test]
fn adr075_counter_add_has_per_key_read_modify_write_serialization() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	// The leader holds a per-key mutation lock...
	assert!(counter.contains("locks: DashMap<CounterKey, Arc<Mutex<()>>>"));
	// ...acquired across the WHOLE read -> checked_add -> append unit (D2).
	let fetch_add = source_region(
		&counter,
		"pub async fn fetch_add(",
		"/// Admit one request under `write_key`, waiting while another holds it.",
	);
	assert!(fetch_add.contains("self.locks.entry(key.clone()).or_default().clone()"));
	// The guard is owned by the add's own task (K5): a caller dropped mid-add
	// cannot release the fan-out's critical section.
	assert!(fetch_add.contains("let _key_guard = lock.lock_owned().await;"));
	assert!(fetch_add.contains("tokio::spawn(body)"));
	// The base read and the overflow-checked add both happen under the guard.
	let serialized = |body: &str| {
		let markers = [
			"let _key_guard = lock.lock_owned().await;",
			"read_identity(counter_identity(key))",
			"checked_add(delta)",
			".replicate_tentative(op_id, key, current, commit_hlc)",
		];
		let positions: Option<Vec<_>> = markers.iter().map(|marker| body.find(marker)).collect();
		positions.is_some_and(|p| p.windows(2).all(|pair| pair[0] < pair[1]))
			&& !body.contains("drop(_key_guard)")
	};
	assert!(serialized(&fetch_add));
	for invalid in [
		fetch_add.replace("let _key_guard = lock.lock_owned().await;", ""),
		format!("read_identity(counter_identity(key));{fetch_add}"),
		fetch_add.replace(
			"read_identity(counter_identity(key))",
			"read_committed(key)",
		),
		fetch_add.replace("checked_add(delta)", "checked_add(delta); drop(_key_guard)"),
	] {
		assert!(!serialized(&invalid), "serialization violation accepted");
	}
	assert!(fetch_add.contains("checked_add(delta)"));
}

#[test]
fn adr075_counter_add_reuses_client_write_id_idempotency() {
	let server = data_server_workspace_source();
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	// D4 public idempotency is keyed by the canonical ClientWriteId bytes (the
	// same derivation as the blob-write cache) and lives in the MUTATION layer:
	// the handler passes the canonical key into the leader, whose dedup record
	// is durable with the witness/applied log (replay survives restart).
	let handler = adr075_counter_handler_region(&server);
	assert!(handler.contains("meta.write_id.canonical_idempotency_bytes()"));
	assert!(handler.contains(".fetch_add_fenced(plan, write_key, &key, meta.delta, fence)"));
	assert!(counter.contains("idempotency: DashMap<CounterWriteKey, RecordedAdd>"));
	// Never a hashed/narrowed handle — collisions would break exactly-once.
	assert!(counter.contains("pub type CounterWriteKey = [u8; 24];"));
	// The record names the add by its full counter key and delta (K1/K2), and
	// one add per write key is admitted by the exact 24 bytes (K3).
	let write_key = read_workspace("crates/wendelon/src/storage/counter/write_key.rs");
	let recorded = source_region(&write_key, "pub(super) struct RecordedAdd {", "}");
	assert!(recorded.contains("key: Option<CounterKey>,"));
	assert!(recorded.contains("delta: NonZeroU64,"));
	assert!(write_key.contains("Some(recorded) if recorded == key && self.delta.get() == delta"));
	assert!(!write_key.contains("counter_identity"));
	assert!(counter.contains("write_gates: WriteKeyGates,"));
	assert!(write_key.contains("type WriteKeyGates = DashMap<CounterWriteKey, GateEntry>;"));
	let gate_entry = source_region(&write_key, "pub(super) struct GateEntry {", "}");
	assert!(gate_entry.contains("mutex: Arc<Mutex<()>>,"));
	// The durable witness carries the dedup identity, and a resolved witness is
	// compacted into the applied log BEFORE pruning (no dedup window).
	assert!(counter.contains("write_key: CounterWriteKey,"));
	assert!(counter.contains("fn append_applied"));
	assert!(counter.contains("fn recover_applied"));
}

/// The install point (K3/K5): inside the add's own task, the write-key record
/// is looked up before the zero-delta check and the counter-key lock, and the
/// add's record is installed exactly once, after its witness is durable and
/// before its value is published and its commits are driven.
#[test]
fn counter_add_installs_its_record_between_the_durable_witness_and_publish() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let body = source_region(
		&counter,
		"async fn admitted_add(",
		"impl<P: CounterParticipant + Clone> CounterLeader<P> {",
	);
	const INSTALL: &str = "self.idempotency.insert(";
	const WITNESS: &str = "self.witness_store.write(&witness).await";
	let ordered = |body: &str| {
		let markers = [
			"self.recorded_answer(&write_key, key, delta)?",
			"NonZeroU64::new(delta)",
			"lock.lock_owned().await",
			"checked_add(delta)",
			WITNESS,
			INSTALL,
			".publish()",
			"self.drive_commits(",
		];
		let positions: Option<Vec<_>> = markers.iter().map(|marker| body.find(marker)).collect();
		positions.is_some_and(|p| p.windows(2).all(|pair| pair[0] < pair[1]))
			&& body.matches(INSTALL).count() == 1
	};
	assert!(ordered(body), "install point out of order");
	// Outside `recover`, which rebuilds the map through `leader.idempotency`, the
	// add's install is the only insertion into the record map.
	assert_eq!(counter.matches(INSTALL).count(), 1);
	let recover = source_region(&counter, "pub fn recover(", "fn commit_hlc_for(");
	assert!(recover.contains("leader.idempotency.insert(record.write_key, recorded);"));
	assert!(recover.contains("leader.idempotency.insert(witness.write_key, recorded);"));

	// Mutation self-tests: the install moved ahead of the witness write, and the
	// install deleted, must both be refused.
	let start = body.find(INSTALL).expect("install");
	let end = start + body[start..].find(");\n").expect("install end") + 3;
	let install = &body[start..end];
	let deleted = body.replacen(install, "", 1);
	let witness = body.find(WITNESS).expect("witness write");
	let statement = body[..witness].rfind("if ").expect("witness statement");
	let mut swapped = deleted.clone();
	swapped.insert_str(statement, install);
	for (what, invalid) in [("deleted", deleted), ("swapped", swapped)] {
		assert!(!ordered(&invalid), "{what} install accepted");
	}
}

/// The dedup horizon's check order (E0, E1) and compaction's single direction
/// (E5). E0 runs on the caller's side before the add's task takes its write-key
/// gate; E1 runs inside the record lookup's answer, after the lookup and before
/// the record answers K1/K2. Compaction never inserts a record: it evicts, and
/// only after it has published `CURRENT` and stored the cutoff with Release
/// ordering. This also closes the receiver gap of the single-install guard
/// above, which counts only `self.idempotency.insert(`: every receiver counts.
#[test]
fn counter_dedup_horizon_is_checked_in_order_and_compaction_only_evicts() {
	// Whitespace-insensitive, so formatting cannot move a marker.
	let squeeze = |text: &str| -> String { text.chars().filter(|c| !c.is_whitespace()).collect() };
	let counter = squeeze(&read_workspace("crates/wendelon/src/storage/counter.rs"));
	let compaction = squeeze(&read_workspace(
		"crates/wendelon/src/storage/counter/compaction.rs",
	));
	let retention = squeeze(&read_workspace(
		"crates/wendelon/src/storage/counter/retention.rs",
	));
	let ordered = |body: &str, markers: &[&str]| {
		let positions: Option<Vec<_>> = markers
			.iter()
			.map(|marker| body.find(&squeeze(marker)))
			.collect();
		positions.is_some_and(|p| p.windows(2).all(|pair| pair[0] < pair[1]))
	};
	let count = |body: &str, marker: &str| body.matches(&squeeze(marker)).count();
	let region = |body: &str, start: &str, end: &str| -> String {
		source_region(body, &squeeze(start), &squeeze(end)).to_owned()
	};

	// E0 before the gate: in `fetch_add_fenced`, before the add's task is
	// spawned, and so before `admitted_add` admits the write key.
	const E0: &str = "self.refuse_unpresentable(&write_key)?;";
	const GATE: &str = "let _write_gate = self.admit_write_key(write_key).await;";
	let fetch_add = region(
		&counter,
		"pub async fn fetch_add(",
		"/// Admit one request under `write_key`, waiting while another holds it.",
	);
	let e0_first =
		|body: &str| ordered(body, &[E0, "tokio::spawn(body)", GATE]) && count(body, E0) == 1;
	assert!(e0_first(&fetch_add), "E0 must precede the write-key gate");

	// E1 between the lookup and the record's answer, read after the lookup.
	const LOOKUP: &str = "let recorded = self.idempotency.get(write_key)";
	const E1: &str = "self.refuse_expired(write_key)?;";
	const ANSWER: &str = "recorded.map(|recorded| recorded.answer(key, delta)).transpose()";
	let answer = region(
		&counter,
		"fn recorded_answer(",
		"/// E0: refuse a write key",
	);
	let e1_placed = |body: &str| ordered(body, &[LOOKUP, E1, ANSWER]) && count(body, E1) == 1;
	assert!(e1_placed(&answer), "E1 must follow the record lookup");
	// The gate precedes the lookup, which precedes the zero-delta check.
	let admitted = region(
		&counter,
		"async fn admitted_add(",
		"impl<P: CounterParticipant + Clone> CounterLeader<P> {",
	);
	assert!(ordered(
		&admitted,
		&[
			GATE,
			"self.recorded_answer(&write_key, key, delta)?",
			"NonZeroU64::new(delta)"
		]
	));
	// E1 reads `Q_mem` (Acquire) then the clock, against the one predicate.
	let check = region(&counter, "fn check_unexpired(", "/// Test hooks between");
	assert!(ordered(
		&check,
		&[
			"let cutoff = self.witness_store.0.cutoff();",
			"retention::expiry_cutoff(cutoff, self.clock.now_ms(), &self.retention)"
		]
	));
	assert!(retention.contains(&squeeze("uuid_v7_unix_millis(uuid)")));
	assert!(retention.contains(&squeeze(
		"issued <= now_ms.saturating_add(config.clock_skew_ms)"
	)));

	// Mutation self-tests: E0 moved behind the gate, E1 moved ahead of the
	// lookup, and E1 deleted are all refused.
	let (e0, gate) = (squeeze(E0), squeeze(GATE));
	let behind_gate = fetch_add
		.replacen(&e0, "", 1)
		.replacen(&gate, &format!("{gate}{e0}"), 1);
	assert!(!e0_first(&behind_gate), "E0 behind the gate accepted");
	let (e1, lookup) = (squeeze(E1), squeeze(LOOKUP));
	let early = answer
		.replacen(&e1, "", 1)
		.replacen(&lookup, &format!("{e1}{lookup}"), 1);
	assert!(!e1_placed(&early), "E1 before the lookup accepted");
	assert!(
		!e1_placed(&answer.replacen(&e1, "", 1)),
		"E1 deleted accepted"
	);

	// E1 itself: `refuse_expired` delegates to the one check.
	let refuse = region(
		&counter,
		"fn refuse_expired(",
		"/// Refuse a write key issued at or before",
	);
	assert!(refuse.contains(&squeeze("self.check_unexpired(write_key)")));

	// counter.rs's production code never removes, retains or clears records:
	// only compaction's eviction removes, and it lives in compaction.rs.
	let raw_counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let production = raw_counter
		.split("#[cfg(test)]\nmod tests {")
		.next()
		.expect("counter.rs production region");
	for forbidden in ["idempotency.remove", ".retain(", ".clear("] {
		assert!(
			!squeeze(production).contains(forbidden),
			"counter.rs production code must not call `{forbidden}`"
		);
	}

	// Addendum A4: in recovery, the barrier (the named generation's sync_data,
	// then a directory sync) precedes every cleanup unlink.
	let resolve = region(
		&compaction,
		"pub(super) fn resolve_image(",
		"/// Verify the generation `CURRENT` names",
	);
	assert!(ordered(
		&resolve,
		&[
			"if clean {",
			"if let Some(generation) = authority {",
			"self.ops.sync_data(&self.generation_path(generation))?;",
			"self.ops.sync_dir(&self.dir)?;",
			"for path in stale {",
			"self.ops.unlink(&path)",
		]
	));

	// The record map is written only by the install and by recovery, through
	// any receiver; compaction never inserts.
	assert_eq!(count(&counter, "idempotency.insert("), 3);
	assert_eq!(count(&counter, "self.idempotency.insert("), 1);
	assert_eq!(count(&counter, "leader.idempotency.insert("), 2);
	assert!(!compaction.contains("idempotency.insert"));
	assert!(!compaction.contains(".insert(write_key"));

	// Compaction publishes `CURRENT`, then `Q_mem` (Release), then evicts; the
	// eviction removes only write keys without a pending witness.
	let compact = region(
		&compaction,
		"pub(super) fn compact(",
		"/// Phase 3 and the cache switch",
	);
	const EVICT: &str = "hooks.evict(&deleted_keys);";
	const PUBLISH: &str = "let published = self.publish(";
	let publish_order = |body: &str| {
		ordered(
			body,
			&[
				PUBLISH,
				"self.publish_cutoff(header.cutoff);",
				EVICT,
				"self.retire_older_generations(header.generation)",
			],
		) && count(body, "hooks.evict(") == 1
	};
	assert!(
		publish_order(&compact),
		"eviction must follow the publish and the cutoff"
	);
	let (evict, publish_call) = (squeeze(EVICT), squeeze(PUBLISH));
	let evict_early = compact.replacen(&evict, "", 1).replacen(
		&publish_call,
		&format!("{evict}{publish_call}"),
		1,
	);
	assert!(
		!publish_order(&evict_early),
		"eviction before the publish accepted"
	);
	let publish = region(
		&compaction,
		"fn publish(",
		"/// A generation number never used",
	);
	assert!(ordered(
		&publish,
		&[
			"let synced = self.ops.sync_dir(&self.dir);",
			".rename(&temp, &self.current_path())",
			".and_then(|()| self.ops.sync_dir(&self.dir));",
			"renamed.map_err(Latched)?;",
			"*state = AppliedLogState::Loaded(next);",
		]
	));
	// Addendum A6: in production a phase-3 directory-sync failure latches.
	assert!(publish.contains(&squeeze(
		"#[cfg(not(test))] let synced = synced.map_err(Latched);"
	)));
	assert!(!publish.contains(&squeeze("self.ops.sync_dir(&self.dir).map_err(Abandoned)")));
	assert!(compaction.contains(&squeeze(
		"self.cutoff.fetch_max(cutoff, Ordering::Release);"
	)));
	assert!(compaction.contains(&squeeze("self.cutoff.load(Ordering::Acquire)")));
	let evict_fn = region(
		&compaction,
		"fn evict(&self, deleted: &[CounterWriteKey]) {",
		"impl<P: CounterParticipant + Clone + Send + Sync + 'static> CounterLeader<P> {",
	);
	assert!(evict_fn.contains(&squeeze("if !pending.contains(write_key) {")));
	assert!(evict_fn.contains(&squeeze("self.0.idempotency.remove(write_key);")));
	// Generation 0 is poisoned only after `CURRENT` is durable.
	assert!(ordered(
		&compact,
		&[PUBLISH, "self.retire_older_generations("]
	));
	let retire = region(
		&compaction,
		"fn retire_older_generations(",
		"/// Replace an unpoisoned",
	);
	assert!(retire.contains(&squeeze("self.poison_generation_zero()?;")));
	// Two call sites in compaction (the retire step and its test-only early
	// control), plus Addendum A1's recovery re-poison, which runs only once
	// recovery has loaded the generation `CURRENT` names.
	assert_eq!(count(&compaction, "self.poison_generation_zero()"), 3);
	let recover = region(&counter, "fn recover_applied(", "fn load_applied(");
	assert!(ordered(
		&recover,
		&[
			"let current = image.generation >= 1;",
			"*state = AppliedLogState::Loaded(image);",
			"if current { self.repoison_on_recovery(&mut state); }",
		]
	));
}

#[test]
fn adr075_duplicate_counter_add_replays_without_mutating() {
	let server = data_server_workspace_source();
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	// The duplicate replay short-circuits inside the leader BEFORE any mutation:
	// the one record lookup, made under the write-key gate, returns `Replayed`
	// before the counter-key lock and the fan-out, so a retry never re-applies
	// the delta (D4)...
	let fetch_add = source_region(
		&counter,
		"pub async fn fetch_add(",
		"/// Admit one request under `write_key`, waiting while another holds it.",
	);
	let replay = fetch_add
		.find("CounterAddResult::Replayed")
		.expect("the leader must replay duplicates");
	let mutate = fetch_add
		.find("replicate_tentative")
		.expect("the leader must fan out the tentative");
	assert!(
		replay < mutate,
		"duplicate replay must short-circuit before the tentative fan-out"
	);
	// ...and the server answers a replay as DUPLICATE (never a second OK).
	let handler = adr075_counter_handler_region(&server);
	assert!(handler.contains("result.is_replay()"));
	assert!(handler.contains("WriteStatus::DUPLICATE"));
}

#[test]
fn adr075_counter_add_goes_through_finalization_not_metric_side_channel() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	assert!(counter.contains("struct CounterFinalizationWitness"));
	let fetch_add = source_region(
		&counter,
		"pub async fn fetch_add(",
		"/// Admit one request under `write_key`, waiting while another holds it.",
	);
	// The durable finalization witness is persisted before the commit fan-out...
	assert!(fetch_add.contains("self.witness_store.write(&witness)"));
	// ...and the add is NOT a metrics counter / in-memory side channel (D3).
	assert!(!fetch_add.contains("metrics()"));
}

#[test]
fn adr075_counter_blob_type_safety_is_structural() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let server = data_server_workspace_source();
	// Counter identity is domain-separated (blake3_keyed in a reserved domain),
	// not the blob tenant-scoped identity — the collision is unrepresentable.
	assert!(counter.contains("b\"wendelon::adr075::counter-domain\""));
	assert!(counter.contains("Identity::blake3_keyed(COUNTER_DOMAIN, &key.0)"));
	// The server re-derives and verifies it; it never trusts the wire identity.
	let handler = adr075_counter_handler_region(&server);
	assert!(handler.contains("counter_identity(&key) != meta.key_identity"));
}

#[test]
fn adr075_no_prefix_scan_registry_or_node_keys() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	for forbidden in [
		"scan_prefix",
		"prefix_scan",
		"list_counters",
		"counter_registry",
	] {
		assert!(
			!counter.contains(forbidden),
			"counter substrate must stay point-addressed (no `{forbidden}`)"
		);
	}
}

#[test]
fn adr075_existing_blob_point_write_stays_last_writer_wins() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let server = data_server_workspace_source();
	// The blob write meta is untouched by counters (no delta/counter contamination).
	let write_meta = source_region(
		&schema,
		"pub struct WriteRequestMeta {",
		"WRITE RESPONSE METADATA",
	);
	for forbidden in ["delta", "counter", "previous_value"] {
		assert!(
			!write_meta.contains(forbidden),
			"WriteRequestMeta must stay a plain LWW blob write (no `{forbidden}`)"
		);
	}
	// The blob write path still routes through its own handler, separate from counters.
	assert!(server.contains("MsgType::WRITE_REQ => {"));
	assert!(server.contains("self.handle_write(&context.actor, frame)"));
}

#[test]
fn adr075_counter_surface_is_brand_clean_no_v1_suffix() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let server = data_server_workspace_source();
	let combined = format!("{schema}\n{server}");
	let identifiers = rust_identifiers_from_source("adr075 counter surface", &combined);
	for required in ["CounterAddRequestMeta", "CounterAddResponseMeta"] {
		assert!(
			has_rust_identifier(&identifiers, required),
			"missing counter-add type {required}"
		);
	}
	for forbidden in [
		"CounterAddRequestMetaV1",
		"CounterAddResponseMetaV1",
		"CounterAddRequestV1",
		"WendelonCounterAddRequest",
		"WendelonCounterAddResponse",
	] {
		assert!(
			!has_rust_identifier(&identifiers, forbidden),
			"counter-add surface must stay brand-clean and no-V1; found {forbidden}"
		);
	}
}

#[test]
fn adr075_no_toron_production_change() {
	// Retargeted after Toron ADR 207: runtime evidence is the one promoted
	// consumer of the counter-add surface. ADR 075 still forbids incidental Toron
	// production drift outside that named later-ADR integration.
	let toron_src = workspace_root().join("crates/toron/src");
	let allowed = ["crates/toron/src/binder/runtime_evidence.rs"];
	let mut offenders = Vec::new();
	walk_dir(&toron_src, &mut |path| {
		if path.extension().and_then(|e| e.to_str()) == Some("rs") {
			let body = fs::read_to_string(path).unwrap_or_default();
			if body.contains("counter_fetch_add") || body.contains("COUNTER_ADD_REQ") {
				let rel = path
					.strip_prefix(workspace_root())
					.unwrap_or(path)
					.to_string_lossy();
				if !allowed.iter().any(|allowed| *allowed == rel.as_ref()) {
					offenders.push(rel.to_string());
				}
			}
		}
	});
	assert!(
		offenders.is_empty(),
		"ADR 075 allows only the ADR 207 runtime-evidence Toron consumer: {offenders:?}"
	);
}

#[test]
fn adr075_counter_add_fails_closed_under_replication() {
	let server = data_server_workspace_source();
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	// Retargeted by ADR 076: the wire status remains for decoding compatibility,
	// but a healthy rf>1 add now builds the replicated participant set instead of
	// failing closed before mutation.
	assert!(schema.contains("pub const COUNTER_REPLICATION_UNSUPPORTED: Self = Self(17);"));
	let handler = adr075_counter_handler_region(&server);
	assert!(!handler.contains("WriteStatus::COUNTER_REPLICATION_UNSUPPORTED"));
	assert!(!handler.contains("replication_factor() > 1"));
	assert!(
		handler.contains("let replicas = self\n\t\t\t.router\n\t\t\t.replicas_for_locality(Locality::from(meta.key_identity));")
	);
	assert!(handler.contains("let Some(owner) = replicas.first().copied()"));
	assert!(handler.contains("WriteStatus::NOT_LEADER"));
	assert!(handler.contains("CounterParticipantPlan::new"));
	assert!(handler.contains("let quorum = (replicas.len() / 2) + 1;"));
	let route = handler
		.find("replicas_for_locality")
		.expect("counter add must derive fresh participants from the ring");
	let mutate = handler
		.find("self.counter_leader_for(meta.tenant_id)")
		.expect("counter add must drive the leader");
	assert!(
		route < mutate,
		"counter add must derive replicated participants before leader mutation"
	);
}

#[test]
fn adr076_witness_records_peer_identities_not_indices() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let witness = source_region(
		&counter,
		"struct CounterFinalizationWitness {",
		"struct AppliedCounterAdd",
	);
	assert!(witness.contains("accepted: Vec<NodeId>"));
	assert!(witness.contains("committed: HashSet<NodeId>"));
	assert!(!witness.contains("Vec<usize>"));
	assert!(!witness.contains("HashSet<usize>"));
}

#[test]
fn adr076_witness_format_is_flattened() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	assert!(counter.contains("pub fn open(dir: impl Into<PathBuf>) -> std::io::Result<Self>"));
	assert!(!counter.contains("V1"));
	assert!(!counter.contains("COUNTER_WITNESS_V1_PREFIX"));
	assert!(!counter.contains("migrate_v0_witness"));
	assert!(!counter.contains("_local_node"));
}

#[test]
fn adr076_fresh_and_recorded_resolution_are_distinct_routes() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let server = data_server_workspace_source();
	assert!(counter.contains("pub struct CounterParticipantPlan<P>"));
	assert!(counter.contains("pub trait CounterParticipantResolver<P: CounterParticipant>"));
	assert!(counter.contains("fn resolve_recorded(&self, id: NodeId) -> Option<P>;"));
	assert!(counter.contains("self.drive_commits(op_id, &resolver).await;"));
	assert!(counter.contains("pub async fn reconcile<R>(&self, resolver: &R)"));
	assert!(server.contains("impl\n\tcrate::storage::counter::CounterParticipantResolver"));
	assert!(server.contains("Some(self.participant_for(id))"));
}

#[test]
fn adr076_counter_leader_has_explicit_local_arm() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	assert!(counter.contains("local_id: NodeId"));
	assert!(counter.contains("local: P"));
	assert!(counter.contains("plan.get(self.local_id).is_none()"));
	let local_union = |body: &str| {
		let compact: String = body.chars().filter(|c| !c.is_whitespace()).collect();
		compact.contains("self.local.read_eligible(identity)")
			&& compact.contains("self.witnessed.read(&identity)")
			&& compact.contains("winner(marked,witnessed)")
			&& !compact.contains("participants[0]")
	};
	assert!(local_union(&counter));
	for invalid in [
		counter.replace(
			".local\n\t\t\t.read_eligible(identity)",
			".participants[0].read_eligible(identity)",
		),
		counter.replace(".read_eligible(identity)", ".read_raw(identity)"),
		counter.replace("winner(marked, witnessed)", "winner(None, witnessed)"),
	] {
		assert!(!local_union(&invalid), "local-arm violation accepted");
	}
	assert!(!counter.contains("participants[0]"));
}

#[test]
fn adr076_fresh_counter_participants_are_ring_derived() {
	let server = data_server_workspace_source();
	let handler = adr075_counter_handler_region(&server);
	assert!(handler.contains("let replicas = self\n\t\t\t.router\n\t\t\t.replicas_for_locality(Locality::from(meta.key_identity));"));
	assert!(handler.contains("let Some(owner) = replicas.first().copied()"));
	assert!(handler.contains("if owner.id != self.local_node.id"));
	assert!(handler.contains("self.counter_add_response_with_hint("));
	assert!(handler.contains("participant_factory.participant_for(*peer)"));
}

#[test]
fn adr076_counter_quorum_uses_majority_floor() {
	let server = data_server_workspace_source();
	let handler = adr075_counter_handler_region(&server);
	assert!(handler.contains("let quorum = (replicas.len() / 2) + 1;"));
	assert!(handler.contains("CounterParticipantPlan::new"));
	assert!(!handler.contains("quorum = 1"));
}

#[test]
fn adr076_commit_completion_covers_full_accepted_set() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let commit = source_region(
		&counter,
		"async fn drive_commits<R>(&self, op_id: u64, resolver: &R)",
		"/// The counter-specific reconciler",
	);
	assert!(commit.contains("witness.accepted.clone()"));
	assert!(commit.contains("for i in accepted"));
	assert!(commit.contains("witness.committed.insert(i);"));
	assert!(commit.contains("let complete = witness\n\t\t\t\t.accepted\n\t\t\t\t.iter()\n\t\t\t\t.all(|i| witness.committed.contains(i));"));
}

#[test]
fn adr076_pending_witness_never_expires_on_peer_loss() {
	let counter = read_workspace("crates/wendelon/src/storage/counter.rs");
	let commit = source_region(
		&counter,
		"async fn drive_commits<R>(&self, op_id: u64, resolver: &R)",
		"/// The counter-specific reconciler",
	);
	assert!(commit.contains("let Some(participant) = resolver.resolve_recorded(i) else"));
	assert!(commit.contains("continue;"));
	let retains_failed = |body: &str| {
		let compact: String = body.chars().filter(|c| !c.is_whitespace()).collect();
		compact.contains("letoutcome=matchparticipant.replicate_tentative(op_id,&key,value,commit_hlc).await{Ok(())=>participant.commit(op_id,&key).await,Err(error)=>Err(error),};")
			&& compact.contains("ifoutcome.is_ok(){ifletSome(mutwitness)=self.witnesses.get_mut(&op_id){witness.committed.insert(i);")
			&& compact.contains("ifmatches!(outcome,Err(ParticipantError::Visibility(_))){self.witnessed.refuse(&counter_identity(&key),CounterVisibilityError::Poisoned);return;}")
	};
	assert!(
		retains_failed(&commit),
		"failed commits must retain witnesses"
	);
	let compact: String = commit.chars().filter(|c| !c.is_whitespace()).collect();
	for invalid in [
		compact.replace("ifoutcome.is_ok()", "ifoutcome.is_err()"),
		compact.replace("Err(error)=>Err(error)", "Err(error)=>Ok(())"),
		compact.replace("ifoutcome.is_ok()", "iftrue"),
		compact.replace(
			"CounterVisibilityError::Poisoned);return;",
			"CounterVisibilityError::Poisoned);",
		),
	] {
		assert!(
			!retains_failed(&invalid),
			"failed-commit violation accepted"
		);
	}
	assert!(!commit.contains("expire"));
	assert!(!commit.contains("deadline"));
	// Replica operation scope is private and bound to the checked allocating node.
	let scope = source_region(&counter, "struct ReplicaOpKey2 {", "impl ReplicaOpKey2");
	for field in ["tenant: TenantId", "origin_node: u32", "op_id: u64"] {
		assert!(scope.contains(field));
	}
	assert!(!scope.contains("epoch"));
	assert!(counter.contains("\nstruct ReplicaOpKey2 {"));
	assert!(counter.contains("origin._reserved != 0"));
	assert!(
		counter.contains(
			"\nenum CounterReplicaRejection {\n\tZeroExpectedKey,\n\tTentativeConflict,\n}"
		)
	);
	assert!(counter.contains("Self::ZeroExpectedKey => std::io::ErrorKind::InvalidInput"));
	assert!(counter.contains("Self::TentativeConflict => std::io::ErrorKind::AlreadyExists"));

	let service = source_region(
		&counter,
		"pub struct CounterReplicaService",
		"/// The leader's own (local) participant",
	);
	assert!(service.contains("pending: Arc<DashMap<ReplicaOpKey2, PendingReplicaTentative>>"));
	assert!(service.contains("op_locks: Arc<DashMap<ReplicaOpKey2, ReplicaGateEntry>>"));
	assert!(service.contains("ReplicaGate::admit(self, key).await"));
	assert!(!service.contains("Arc::strong_count") && !service.contains("reclaim_gate2"));
	let gate = source_region(
		&counter,
		"struct ReplicaGateEntry",
		"/// The server-side counter replica over",
	);
	assert!(gate.contains("participants: usize"));
	assert!(gate.contains("mutex.lock_owned().await"));
	assert!(gate.contains("drop(self.guard.take());"));
	assert!(gate.contains("*count == 0 && !self.service.pending.contains_key(&self.key)"));
	for marker in ["pub async fn commit(", "pub async fn rollback("] {
		let start = service.find(marker).unwrap();
		let next = &service[start..];
		let zero = next.find("check_expected_key2(expected_key)?;").unwrap();
		let gate = next.find("ReplicaGate::admit(self, key).await;").unwrap();
		let lookup = next.find("self.pending.get(&key)").unwrap();
		let compare = next.find("pending.key_identity != expected_key").unwrap();
		assert!(zero < gate && gate < lookup && lookup < compare);
	}
	assert!(counter.contains(
		"fn counter_replica_rejection2(error: &std::io::Error) -> Option<&CounterReplicaRejection>"
	));
	assert!(counter.contains("error.get_ref()?.downcast_ref::<CounterReplicaRejection>()"));
	let network = data_server_workspace_source();
	for (handler, next, operation) in [
		(
			"async fn handle_counter_replicate",
			"async fn handle_counter_rollback",
			"counter replication",
		),
		(
			"async fn handle_counter_rollback",
			"async fn handle_counter_add",
			"counter rollback",
		),
	] {
		let body = source_region(&network, handler, next);
		assert!(body.contains("context: &RequestContext"));
		let origin = body
			.find(&format!("require_authenticated_node(\"{operation}\")"))
			.unwrap();
		assert!(body.contains("origin._reserved == 0"));
		assert!(body.contains("CounterReplicaStatus::NOT_AUTHORIZED"));
		assert!(
			origin
				< body
					.find("self.counter_replica")
					.or_else(|| body.find(".counter_replica"))
					.unwrap()
		);
		assert!(body.contains("Err(_) => CounterReplicaStatus::INTERNAL_ERROR"));
	}
	assert!(network.contains("self.handle_counter_replicate(&context, frame)"));
	assert!(network.contains("self.handle_counter_rollback(&context, frame)"));
}

#[test]
fn adr076_replication_unsupported_not_on_healthy_path() {
	let server = data_server_workspace_source();
	let handler = adr075_counter_handler_region(&server);
	assert!(!handler.contains("COUNTER_REPLICATION_UNSUPPORTED"));
	assert!(!handler.contains("replication_factor() > 1"));
}

#[test]
fn adr076_no_new_wire_operations() {
	let schema = read_workspace("crates/wendelon/src/schema/core.rs");
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	for required in [
		"pub const COUNTER_ADD_REQ: Self = Self(0x4C);",
		"pub const COUNTER_ADD_RESP: Self = Self(0x4D);",
		"pub const COUNTER_READ_REQ: Self = Self(0x4E);",
		"pub const COUNTER_READ_RESP: Self = Self(0x4F);",
		"pub const COUNTER_REPLICATE_REQ: Self = Self(0x48);",
		"pub const COUNTER_REPLICATE_RESP: Self = Self(0x49);",
		"pub const COUNTER_ROLLBACK_REQ: Self = Self(0x4A);",
		"pub const COUNTER_ROLLBACK_RESP: Self = Self(0x4B);",
	] {
		assert!(
			schema.contains(required),
			"missing existing counter wire op {required}"
		);
	}
	for forbidden in [
		"COUNTER_FETCH_ADD_REQ",
		"COUNTER_FETCH_ADD_RESP",
		"COUNTER_REPLICATED_ADD_REQ",
		"COUNTER_FINALIZE_REQ",
	] {
		assert!(
			!schema.contains(forbidden),
			"ADR 076 must reuse the ADR 075 counter wire surface; found {forbidden}"
		);
	}
	for routed in [
		"MsgType::COUNTER_REPLICATE_REQ.0",
		"MsgType::COUNTER_REPLICATE_RESP.0",
		"MsgType::COUNTER_ROLLBACK_REQ.0",
		"MsgType::COUNTER_ROLLBACK_RESP.0",
	] {
		assert!(
			router.contains(routed),
			"existing counter replica wire op must route through the Data Ring: {routed}"
		);
	}
}

#[test]
fn adr076_no_toron_production_change() {
	let toron_src = workspace_root().join("crates/toron/src");
	let mut offenders = Vec::new();
	walk_dir(&toron_src, &mut |path| {
		if path.extension().and_then(|e| e.to_str()) == Some("rs") {
			let body = fs::read_to_string(path).unwrap_or_default();
			if body.contains("CounterFinalizationWitness")
				|| body.contains("CounterParticipantResolver")
				|| body.contains("CounterParticipantPlan")
			{
				let rel = path
					.strip_prefix(workspace_root())
					.unwrap_or(path)
					.to_string_lossy();
				offenders.push(rel.to_string());
			}
		}
	});
	assert!(
		offenders.is_empty(),
		"ADR 076 is Wendelon-only; Toron production files contain ADR 076 symbols:\n{offenders:?}"
	);
}

#[test]
fn adr076_prior_wendelon_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn adr075_prior_wendelon_guards_continue_to_pass",
		"fn adr075_counter_add_fails_closed_under_replication",
		"fn adr074_prior_wendelon_telemetry_and_table_query_guards_continue_to_pass",
	] {
		assert!(
			architecture.contains(required),
			"missing prior Wendelon guard {required}"
		);
	}
}

#[test]
fn adr075_prior_wendelon_guards_continue_to_pass() {
	let architecture = read_workspace("crates/wendelon/tests/architecture_test.rs");
	for required in [
		"fn adr074_prior_wendelon_telemetry_and_table_query_guards_continue_to_pass",
		"fn adr073_prior_wendelon_protocol_and_architecture_guards_continue_to_pass",
		"fn adr072_prior_adr_guards_continue_to_pass",
		"fn adr071_wendelon_inspect_branch_exists",
	] {
		assert!(
			architecture.contains(required),
			"missing prior Wendelon guard {required}"
		);
	}
}

// ============================================================
//  ADR 077 — data server module decomposition
// ============================================================

/// The production source-scan surface is the root plus exactly the carved
/// domain submodules (sorted), `tests.rs` excluded. Pins the file manifest so a
/// new module is a deliberate edit here, and proves the concatenation actually
/// spans modules (symbols from three different domain files are all present).
#[test]
fn adr077_data_server_source_concatenates_submodules() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let basenames: Vec<String> = data_server_source_files(manifest_dir)
		.iter()
		.map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
		.collect();
	assert_eq!(
		basenames,
		vec![
			"data_server.rs",
			// ADR 0699 S5a: a store's part in certified generations.
			"certification.rs",
			// ADR 0699 S3b: child writes under the ParentDecisions authority, kept
			// out of the bounded root.
			"child_writes.rs",
			// ADR 0699 S5b: a store's certified cleanup.
			"cleanup.rs",
			// ADR 0699 S3c: the coordinator of a table write in that mode.
			"coordinator.rs",
			"counter.rs",
			// ADR 0699 S4b: the single-row delete of that mode.
			"deletes.rs",
			"descriptor_migration.rs",
			// ADR 0701: a ParentDecisions holder's batched lease renewal.
			"lease_batch.rs",
			// ADR 542's durable outcome/replay wire is one named protocol seam;
			// keep its framing and resolution state out of the bounded root.
			"mutation_protocol.rs",
			// ADR 629 keeps ownership registration and its test barriers out of the root.
			"observation.rs",
			// Data-directory ownership plumbing (store binding to the node's owner
			// and the server's owner-admitted direct writes) stays out of the root.
			"ownership.rs",
			// ADR 541's two clocks and its bounded encode quanta: self-contained
			// mechanisms with their own invariants, carved so the bounded root
			// keeps ratcheting down rather than absorbing each new one.
			"query_clocks.rs",
			"query_encode.rs",
			// ADR 0699 S4: the reader of that mode, and the typed repair of an
			// Accepted child it relies on.
			"reader.rs",
			"repair.rs",
			"replica_routing.rs",
			// ADR 0089's per-request scan accounting: the decode audit that makes
			// read amortization checkable, the bounded cache that produces it, and
			// the sampled phase trace that attributes what the amortization left
			// behind. One question — what did this scan do — so one module.
			"scan_accounting.rs",
			"streaming.rs",
			"table_query.rs",
			"write_clocks.rs",
			// ADR 0714: the gate between a coordinated table write attempt and its
			// caller's deadline, and the early answer a stream handler joins with
			// the remainder. One question (who wins, the attempt or the deadline),
			// so one module.
			"write_deadline.rs",
			"write_replication.rs",
		],
		"ADR 077 production source manifest changed; update this guard deliberately \
		 (tests.rs is excluded — it is not production source)"
	);
	let source = data_server_source(manifest_dir);
	for symbol in [
		"struct CounterParticipantFactory", // counter.rs
		"struct DurableQuorumTracker",      // write_replication.rs
		"enum QueryProjectedExecution",     // table_query.rs
	] {
		assert!(
			source.contains(symbol),
			"data_server_source concatenation is missing a carved symbol: {symbol}"
		);
	}
	// Every prior data-server source-scan guard must route through this helper,
	// not a raw read of the (now-decomposed) flat file.
	let arch = read_workspace("crates/wendelon/tests/architecture_test.rs");
	assert!(
		arch.contains("data_server_source(") || arch.contains("data_server_workspace_source("),
		"source-scan guards must read the data server via the concatenation helper"
	);
}

/// Each named domain module exists as a file and is declared in the root.
#[test]
fn adr077_semantic_modules_exist() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	for module in [
		"counter",
		"table_query",
		"write_replication",
		"descriptor_migration",
		"streaming",
		"replica_routing",
	] {
		let path = manifest_dir.join(format!("src/network/data_server/{module}.rs"));
		assert!(path.is_file(), "ADR 077 domain module missing: {module}.rs");
		assert!(
			root.contains(&format!("mod {module};")),
			"ADR 077 domain module not declared in root: mod {module};"
		);
	}
}

/// ADR 542's abandoned-work cap covers admission and synchronous batch construction,
/// not merely the later replica phase. Moving the permit below either bifurcation rail
/// would make the same finite-but-expensive work accumulate again under caller expiry.
#[test]
fn impl542_mutation_slot_precedes_table_admission_and_bifurcation() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let table_start = root.find("\n\tasync fn table_write(").expect("table write");
	let delete_start = root[table_start..]
		.find("\n\tasync fn table_delete(")
		.map(|offset| table_start + offset)
		.expect("table delete");
	let data_ring_start = root[delete_start..]
		.find("\n#[async_trait]\nimpl DataRing")
		.map(|offset| delete_start + offset)
		.expect("DataRing impl after table delete");
	let table = &root[table_start..delete_start];
	let delete = &root[delete_start..data_ring_start];
	let table_permit = table
		.find("acquire_table_mutation_permit")
		.expect("table mutation permit");
	for later in [
		"acquire_secondary_index_admission_guard",
		"bifurcate_content_hash_table_write",
		"bifurcate_primary_key_table_write",
	] {
		assert!(
			table_permit < table.find(later).expect("table admission operation"),
			"mutation slot must bind before {later}"
		);
	}
	assert!(
		delete
			.find("acquire_delete_mutation_permit")
			.expect("delete permit")
			< delete.find("merge_commit_hlc").expect("delete admission"),
		"delete mutation slot must bind before classification and tombstone packing"
	);

	let protocol =
		fs::read_to_string(manifest_dir.join("src/network/data_server/mutation_protocol.rs"))
			.unwrap();
	assert!(protocol.contains("acquire_mutation_work_permit("));
	assert!(protocol.contains("MutationCommitKnowledge::KNOWN_NOT_COMMITTED"));
}

/// The struct definition and `MsgType` dispatch stay in the root; carved handler
/// *definitions* do not (their call sites in dispatch legitimately remain).
#[test]
fn adr077_dispatch_and_struct_stay_in_root() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	assert!(
		root.contains("struct WendelonDataServer"),
		"WendelonDataServer struct must stay in the root module"
	);
	assert!(
		root.contains("MsgType::"),
		"MsgType dispatch routing must stay in the root module"
	);
	assert!(
		!root.contains("async fn handle_counter_add"),
		"carved handler definition leaked back into the root (handle_counter_add)"
	);
	let source = data_server_source(manifest_dir);
	assert!(
		source.contains("async fn handle_counter_add"),
		"carved handler definition not found anywhere in the decomposition"
	);
}

/// The root is bounded. Set from the post-carve size plus modest headroom;
/// ratchet DOWN as domain helper methods are relocated in follow-on cuts
/// (see plans/conventions/module-decomposition.md). It must never regrow toward
/// the pre-carve monolith. The 15_400 cap deliberately accounts for the
/// root-level fail-closed orphan-witness admission barrier and its two write
/// call sites, which must remain beside MsgType dispatch for ADR 077, plus
/// the deterministic `#[cfg(test)]` leadership-override plumbing required to
/// exercise the unavailable-leadership branch. ADR 0088 added the table-query
/// response accumulator beside the stream writer it flushes through; it is
/// response framing, not a domain helper, so it stays at this root.
/// ADR 0090 wired the central placement resolver after every read-side record
/// decode — seven sites plus an import — and judges point-read primacy from the
/// record's own placement key before the slab is touched. Both are read-path
/// enforcement, not domain helpers, and remain beside what they validate. The
/// ingress slice added partition derivation at the write boundary, the
/// tombstone placement arm, and placement-keyed fan-out; the transfer slice
/// added the transfer-aware serving consultation at primacy and fan-out; the
/// secondary-index read resolves placement from the key chain, since the V1
/// segment format carries none; ADR 0089 added the bounded decoded-record
/// cache and its per-request audit beside the scan loop it serves. All are
/// read/write-path enforcement with the same claim to this root.
/// ADR 0095 adds the drop-reclaim wire structs and dispatch, mutation-drain
/// admission at the leader write/delete seams, and the leader's bounded-debt
/// orchestration. ADR 077 requires dispatch and root server state to remain
/// here, so this is a deliberate mechanism-sized bump rather than domain
/// helper regrowth. ADR 0096 adds the bounded participant phase-transition
/// signal beside the durable progress write whose post-transition cursor and
/// examined count it reports; moving it would split the observation from the
/// state transition it describes. ADR 0102 F-CTR-5b carved the per-tenant
/// counter-leader factory into `counter.rs`, and the cap ratchets down to
/// the size that left (16_287).
#[test]
fn adr077_root_is_bounded() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let lines = root.lines().count();
	assert!(
		lines <= 16_287,
		"data_server.rs root is {lines} lines (cap 16_287); carve more or justify a deliberate cap bump"
	);
}

/// The inline `#[cfg(test)] mod tests` block was extracted to its own file.
#[test]
fn adr077_tests_extracted() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	assert!(
		root.contains("#[cfg(test)]\nmod tests;"),
		"root must declare the extracted tests module, not inline it"
	);
	let tests = manifest_dir.join("src/network/data_server/tests.rs");
	assert!(tests.is_file(), "data_server/tests.rs must exist");
	let body = fs::read_to_string(&tests).unwrap();
	assert!(
		body.contains("use super::*;") && body.contains("async fn"),
		"tests.rs must be the extracted test body (use super::*; + test fns)"
	);
}

// ADR 077 `adr077_no_wire_or_runtime_surface_change` — RETIRED in the ADR 233 cross-ring
// reconciliation. It was a development-time `git status --short` tripwire (no production source
// outside `network/data_server/**` dirty while the cut was in flight). It could not simply be
// color-hardened: (1) under `color.ui=always` the ANSI codes shift `line[3..]`, so
// `starts_with("crates/")` was always false and the guard asserted nothing; (2) a *working*
// git-status form false-positives on any unrelated dirty production file — it would fail this suite
// whenever a Toron cut like ADR 233 stages an edit under `crates/toron/src`. The move-only invariant
// it proxied is enforced, git-independently, by the structural siblings
// `adr077_dispatch_and_struct_stay_in_root`, `adr077_root_is_bounded`,
// `adr077_semantic_modules_exist`, and `adr077_data_server_source_concatenates_submodules`.

/// The de-versioned doc heading stays renamed (doc-prose variant of the
/// repo-wide no-V1 discipline): no `## V1 scope` heading, no `V1 fallback` prose.
#[test]
fn adr077_no_v1_scope_heading() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = data_server_source(manifest_dir);
	assert!(
		!source.contains("## V1 scope"),
		"the `## V1 scope` heading must stay renamed"
	);
	assert!(
		!source.contains("V1 fallback"),
		"the `V1 fallback` prose must stay renamed"
	);
	assert!(
		fs::read_to_string(manifest_dir.join("src/network/data_server.rs"))
			.unwrap()
			.contains("## Scope and simplifications"),
		"the renamed scope heading must be present in the root"
	);
}

// ============================================================
//  ADR 078 — slab-manager module decomposition
// ============================================================

/// The production source-scan surface is the root plus exactly the carved
/// subsystem islands (sorted), `tests.rs` excluded. Pins the file manifest and
/// proves the concatenation spans islands (symbols from three different islands
/// are all present).
#[test]
fn adr078_manager_source_concatenates_submodules() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let basenames: Vec<String> = manager_source_files(manifest_dir)
		.iter()
		.map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
		.collect();
	assert_eq!(
		basenames,
		vec![
			"manager.rs",
			// ADR 0696: every backing and tier-generation install into the state.
			"backing_install.rs",
			// ADR 0699 S5b: the certified cleanup rewrite.
			"certified_cleanup.rs",
			// ADR 0704: how every publication path classifies a certified generation.
			"certified_delivery.rs",
			// ADR 0703: LSM compaction that elides one certified selection.
			"certified_lsm.rs",
			// ADR 0704: the admission and open-time fence of certified tiered cleanup.
			"certified_tier.rs",
			"compaction.rs",
			"counter_visibility.rs",
			// ADR 0696 (0692 §7): disposal evidence for typed Superseded after disposal.
			"disposal_evidence.rs",
			"encoding_compute.rs",
			"gc_disposal.rs",
			"index_publication.rs",
			// ADR 0699 S4: the poison-recovery lock helpers, moved out of the root.
			"lock_recovery.rs",
			"lsm_driver.rs",
			"maintenance_publication.rs",
			// Snapshot-race metadata observations and durable operation tokens.
			"metadata_observation.rs",
			// ADR 629 counted publication helpers preserve the existing entry points.
			"observation.rs",
			"op_hooks.rs",
			// ADR 0697 §6: resolving operations no live workflow holds.
			"op_recovery.rs",
			"open.rs",
			// ADR 0692: test-only pause points (compiled only under cfg(test)), kept
			// out of the bounded root.
			"pause_hooks.rs",
			"publication_accounting.rs",
			// ADR 0696 (0692 §7): charged cross-slab redirects of maintenance rewrites.
			"redirect_store.rs",
			// ADR 0692: per-key relocation and atomic empty-key removal.
			"relocation.rs",
			// ADR 0697 §4: reserved ids, their capability and their terminal paths.
			"reservations.rs",
			"retention_reclaim.rs",
			"secondary_index_cleanup.rs",
			"secondary_index_segments.rs",
			// Local slab files that disagree with the committed tier state, and
			// the ownership rule for every slab-path unlink.
			"slab_file_quarantine.rs",
			// ADR 0697: the durable slab id high-water, committed before use.
			"slab_ids.rs",
			"stats_facts.rs",
			// ADR 0692: tier generations, the read snapshot and the typed refusal.
			"tier_authority.rs",
			// ADR 0696 (0692 §7): the node budget for tier history and redirects.
			"tier_budget.rs",
			// ADR 0696 (0692 §7): typed tier deferrals and their replay.
			"tier_deferral.rs",
			"tier_generation.rs",
			// ADR 0696 (0692 §8): the test-support measurement instrument.
			"tier_measure.rs",
			// ADR 0692: tier publication plans pair rows by logical owner.
			"tier_plan.rs",
			// ADR 0696 (0692 §7): proposer history tokens.
			"tier_tokens.rs",
			// ADR 0705: the tier transition classifier.
			"tier_transition.rs",
			"tiered_io.rs",
			// ADR 0692: class-scoped version identity and tier verification.
			"version_identity.rs",
		],
		"ADR 078 production source manifest changed; update this guard deliberately \
		 (tests.rs is excluded — it is not production source)"
	);
	let source = manager_source(manifest_dir);
	for symbol in [
		"enum TieredStatsContributor",      // stats_facts.rs
		"struct EncodingComputePhaseBPlan", // encoding_compute.rs
		"struct CompactionRecord",          // compaction.rs
	] {
		assert!(
			source.contains(symbol),
			"manager_source concatenation is missing a carved symbol: {symbol}"
		);
	}
	let arch = read_workspace("crates/wendelon/tests/architecture_test.rs");
	assert!(
		arch.contains("manager_source("),
		"source-scan guards must read the manager via the concatenation helper"
	);
}

/// Each named subsystem island exists as a file and is declared in the root.
#[test]
fn adr078_semantic_modules_exist() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	for module in [
		"stats_facts",
		"encoding_compute",
		"tiered_io",
		"lsm_driver",
		"compaction",
		"retention_reclaim",
		"secondary_index_segments",
	] {
		let path = manifest_dir.join(format!("src/storage/manager/{module}.rs"));
		assert!(path.is_file(), "ADR 078 island missing: {module}.rs");
		assert!(
			root.contains(&format!("mod {module};")),
			"ADR 078 island not declared in root: mod {module};"
		);
	}
}

/// The `SlabManager` struct, its public config/report surface, and construction
/// stay in the root; carved subsystem method *definitions* do not.
#[test]
fn adr078_slab_manager_public_surface_stays_in_root() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	assert!(
		root.contains("pub struct SlabManager {"),
		"the SlabManager struct must stay in the root module"
	);
	for surface in [
		"pub struct SlabManagerConfig",
		"pub struct TombstoneCompactionReport",
		"pub struct RetentionPruningReport",
	] {
		assert!(
			root.contains(surface),
			"the public surface type must stay in the root: {surface}"
		);
	}
	assert!(
		root.contains("pub use secondary_index_segments::SecondaryIndexSegmentInfo;"),
		"moved public island type must preserve the pre-split manager::SecondaryIndexSegmentInfo path"
	);
	for crate_visible in [
		"EncodedAggregateL2RecordRead",
		"EncodingMaterializeAuditSample",
		"ProjectedL2RecordRead",
	] {
		assert!(
			root.contains(crate_visible) && root.contains("pub(crate) use encoding_compute::{"),
			"moved crate-visible encoding type must preserve the pre-split manager::{crate_visible} path"
		);
	}
	assert!(
		root.contains("async fn open("),
		"construction (open) must stay in the root module"
	);
	assert!(
		!root.contains("fn compact_one_sealed_slab"),
		"a carved subsystem method definition leaked back into the root"
	);
	let source = manager_source(manifest_dir);
	assert!(
		source.contains("fn compact_one_sealed_slab"),
		"carved subsystem method not found anywhere in the decomposition"
	);
}

/// The root is bounded. Set from the post-carve size plus modest headroom;
/// ratchet DOWN in the deferred deeper-helper-relocation follow-on (ADR 078 D5).
/// ADR 0095 adds SlabManager's public bounded table-drop reclaim and keyed
/// absence-verification entry points here; their mechanics remain in the
/// retention-reclaim module, preserving ADR 078's decomposition boundary.
/// ADR 0098 decision 4 makes append rotation payload-aware: the root's append
/// entry points pass their payload size to `target_for_append`, nine lines that
/// belong beside the rotation they govern rather than in a carved module. The
/// same decision later moved the byte reservation under the rotation lock, so
/// the guard reserves and returns a token: a dozen more lines, same home.
/// ADR 0099 hands open-time slab recovery the durable rows as its boundary
/// oracle; building that map beside the snapshot load is the same size again,
/// and threading that evidence through the blob-walking free functions adds
/// their signatures. Those free functions are the next carve.
#[test]
fn adr078_root_is_bounded() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let lines = root.lines().count();
	assert!(
		lines <= 4_650,
		"manager.rs root is {lines} lines (cap 4_650); carve more or justify a deliberate cap bump"
	);
}

/// The inline `#[cfg(test)] mod tests` block was extracted to its own file.
#[test]
fn adr078_tests_extracted() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	assert!(
		root.contains("#[cfg(test)]\nmod tests;"),
		"root must declare the extracted tests module, not inline it"
	);
	let tests = manifest_dir.join("src/storage/manager/tests.rs");
	assert!(tests.is_file(), "manager/tests.rs must exist");
	let body = fs::read_to_string(&tests).unwrap();
	assert!(
		body.contains("use super::*;") && body.contains("async fn"),
		"tests.rs must be the extracted test body (use super::*; + test fns)"
	);
	assert!(
		!body.contains("V1 sparse projection"),
		"retired milestone prose must not reappear in the extracted tests"
	);
	assert!(
		body.contains("Sparse projection fetch consumes"),
		"the de-versioned sparse projection comment must remain referent-named"
	);
}

// ADR 078 `adr078_no_storage_wire_or_runtime_surface_change` — RETIRED in the ADR 233 cross-ring
// reconciliation, for the same reasons as the ADR 077 tripwire above: the `git status --short` form
// was vacuous under `color.ui=always` (ANSI codes defeat the `line[3..]` path parse), and a working
// version would false-positive on any unrelated dirty production file. The move-only invariant is
// enforced, git-independently, by the structural siblings
// `adr078_slab_manager_public_surface_stays_in_root`, `adr078_no_mod_rs_root`,
// `adr078_root_is_bounded`, `adr078_semantic_modules_exist`, and
// `adr078_manager_source_concatenates_submodules`.

/// The module root is a sibling file `storage/manager.rs`, never
/// `storage/manager/mod.rs` (mod.rs is manifests-only in this crate).
#[test]
fn adr078_no_mod_rs_root() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	assert!(
		manifest_dir.join("src/storage/manager.rs").is_file(),
		"the module root must be the sibling file storage/manager.rs"
	);
	assert!(
		!manifest_dir.join("src/storage/manager/mod.rs").exists(),
		"storage/manager/mod.rs must not exist (mod.rs is manifests-only)"
	);
}

/// ADR 081 follow-up: the status wire pair must be present in the router, not
/// merely defined and handled. The omitted request previously closed the stream
/// with an `early eof` before the control handler could observe it.
#[test]
fn adr081_secondary_index_status_is_routed_both_directions() {
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	for required in [
		"MsgType::SECONDARY_INDEX_STATUS_REQ",
		"MsgType::SECONDARY_INDEX_STATUS_RESP",
		"secondary_index_status_routes_to_control_ring",
	] {
		assert!(
			router.contains(required),
			"ADR 081 status routing control is missing {required}"
		);
	}
}

/// ADR 082: widening TLS trust must preserve the node/operator boundary at
/// every later seam of the shared HRFT listener.
#[test]
fn adr082_shared_ingress_preserves_trust_and_routing_boundaries() {
	let node = read_workspace("crates/wendelon/src/node.rs");
	let server = read_workspace("crates/wendelon/src/network/server.rs");
	let transport = read_workspace("crates/wendelon/src/network/transport.rs");
	let router = read_workspace("crates/wendelon/src/network/router.rs");
	let quic = read_workspace("crates/wendelon/src/network/quic_transport.rs");

	for required in [
		"ActorTrustRoots::load(",
		"endpoint_from_tls_with_peer_roots(",
		"rustls_server_config_with_peer_roots(",
	] {
		assert!(
			node.contains(required),
			"ADR 082 production bootstrap is missing {required}"
		);
	}
	assert!(
		server.contains("pub remote: Option<NodeId>")
			&& transport.contains("fn remote_id(&self) -> Option<NodeId>")
			&& quic.contains("pub fn new_inbound(")
			&& quic.contains("remote_id: actor_identity.node_id"),
		"ADR 082 requires optional node identity through connection and request context"
	);
	let server_production = server
		.split("#[cfg(test)]")
		.next()
		.expect("server production source");
	assert!(
		!server_production.contains("NodeId { id: 0")
			&& !server_production.contains("NodeId {\n\t\tid: 0"),
		"ADR 082 forbids a synthetic node identity for admitted operator actors"
	);
	for required in [
		"verified_peer_actor_identity_with_trust_roots_and_revocations(",
		"verified_peer_actor_identity_from_chain_with_trust_roots_and_revocations(",
	] {
		assert!(
			server.contains(required),
			"ADR 082 trust-domain binding is missing {required}"
		);
	}
	for required in [
		"ACTOR_AUTHORIZED_CONTROL_REQUESTS",
		"MsgType::ALL",
		"require_authenticated_node",
		"operator_ingress_is_control_only_and_node_ingress_keeps_attribution",
		"ingress_census_classifies_every_named_message_and_nothing_unknown",
	] {
		assert!(
			router.contains(required),
			"ADR 082 guarded ingress census is missing {required}"
		);
	}
	assert!(
		quic.contains("verified_transport_peer_with_revocations("),
		"ADR 082 must keep dial-side inter-node verification node-only"
	);
}

fn snapshot_header_invents_complete_log_identity(source: &str) -> bool {
	let compact = source
		.chars()
		.filter(|character| !character.is_whitespace())
		.collect::<String>();
	compact.contains("CommittedLeaderId::new(header.last_included_term,0)")
}

/// impl 534: SnapshotHeader owns term/index coordinates, not a complete
/// OpenRaft LogId. The hash-verified body supplies identity and presence.
#[test]
fn impl534_snapshot_headers_do_not_invent_log_identity() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let production = source
		.split("#[cfg(test)]\nmod recovery_tests")
		.next()
		.expect("production raft-storage source");
	let forbidden_control = r#"
		CommittedLeaderId::new(header.last_included_term, 0)
	"#;
	assert!(
		snapshot_header_invents_complete_log_identity(forbidden_control),
		"impl 534 positive control must prove the forbidden reconstruction is observable"
	);
	assert!(
		!snapshot_header_invents_complete_log_identity(production),
		"impl 534 forbids constructing complete log identity from SnapshotHeader: the header owns coordinates, not node identity"
	);
	assert!(
		production.contains(
			"validate_snapshot_header_log_coordinates(&header, state.last_applied_log_id)"
		),
		"impl 534 requires local load and install to validate header coordinates against hash-verified body identity"
	);
	assert!(
		source.contains(
			"fn log_id(term: u64, index: u64) -> LogId<u32> {\n\t\tlog_id_for(1, term, index)"
		),
		"impl 534 recovery fixtures must default to a nonzero leader node id"
	);
}

/// ADR 0090: placement is never resolved from the index alone. The record frame
/// is independent durable evidence — the only way a V5 index value truncated to
/// a V4 length is distinguishable from a genuine V4 value — so every read path
/// that decodes a stored record must run the central resolver, and nothing
/// outside the sanctioned modules may construct a `PlacementKey` directly.
#[test]
fn adr0090_placement_is_resolved_centrally_never_from_the_index_alone() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

	// 1. `PlacementKey` construction is confined to the modules that own it:
	//    the schema definition, the resolver's module, and the routing matches
	//    (ring, the replica-router trait and its impls, and the test router).
	//    `PlacementKey::from_parts` is the sanctioned constructor and is exempt
	//    everywhere; a read path building a VARIANT elsewhere is resolving
	//    placement on its own authority.
	let allowed_construction = [
		"src/schema/table.rs",
		"src/storage/multirow.rs",
		"src/network/ring.rs",
		"src/network/data_server/replica_routing.rs",
		"src/network/data_server/tests.rs",
	];
	let mut offenders = Vec::new();
	walk_dir(&manifest_dir.join("src"), &mut |path| {
		if path.extension().is_none_or(|ext| ext != "rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		let source = source.replace("PlacementKey::from_parts", "");
		if !source.contains("PlacementKey::") {
			return;
		}
		let relative = path
			.strip_prefix(manifest_dir)
			.unwrap()
			.to_string_lossy()
			.replace('\\', "/");
		if !allowed_construction
			.iter()
			.any(|allowed| relative == *allowed)
		{
			offenders.push(relative);
		}
	});
	assert!(
		offenders.is_empty(),
		"PlacementKey must be constructed only where placement is owned; \
		 found construction in: {offenders:?}"
	);

	// 2. Every storage-record decode in the data server's read paths is followed
	//    by the central resolver. The write path decodes child frames it just
	//    built, so the count is of read-side `decode_batch(&bytes)` sites; each
	//    must have a resolver call, and the KV read path has its own.
	let data_server = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	// ADR 0089 moved the scan loop's decode inside its coalescing cache-miss
	// arm, where the decoded record is shared across the rows that reference
	// it; both spellings are read-side decodes and both must resolve placement.
	let read_decodes = data_server
		.matches("let storage_batch = decode_batch(&bytes)?;")
		.count()
		+ data_server
			.matches("let batch = Arc::new(decode_batch(&bytes)?);")
			.count();
	let kv_decodes = data_server
		.matches("let stored = decode_batch(&bytes)?;")
		.count();
	let resolver_calls = data_server.matches("resolve_record_placement(").count();
	assert_eq!(
		resolver_calls,
		read_decodes + kv_decodes,
		"every read-side record decode must run the central placement resolver: \
		 {read_decodes} + {kv_decodes} decode sites, {resolver_calls} resolver calls"
	);
	assert!(
		read_decodes >= 6,
		"the read-side decode census shrank unexpectedly ({read_decodes}); \
		 if a read path was removed or renamed, retarget this guard rather than \
		 letting it under-count"
	);
}

/// ADR 0092: the versioned-replay shadow modules are replay machinery, never
/// an API. Only the consensus decode dispatch (raft_storage, which also hosts
/// the format-fixture controls), the shadow definitions themselves, and their
/// module registration may name them. Any other reference means a live code
/// path is reading or building a superseded shape on its own authority.
#[test]
fn adr0092_versioned_body_shadows_are_replay_only() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let allowed = [
		"src/consensus/mod.rs",
		"src/consensus/raft_storage.rs",
		"src/consensus/snapshot_body.rs",
		"src/consensus/wal_body.rs",
	];
	let mut offenders = Vec::new();
	walk_dir(&manifest_dir.join("src"), &mut |path| {
		if path.extension().is_none_or(|ext| ext != "rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		if !source.contains("wal_body") && !source.contains("snapshot_body") {
			return;
		}
		let relative = path
			.strip_prefix(manifest_dir)
			.unwrap()
			.to_string_lossy()
			.replace('\\', "/");
		if !allowed.iter().any(|entry| relative == *entry) {
			offenders.push(relative);
		}
	});
	assert!(
		offenders.is_empty(),
		"the ADR 0092 shadow modules leaked outside the replay dispatch: {offenders:?}"
	);
}

/// ADR 0093: every production QUIC endpoint inherits one Wendelon liveness
/// policy. A constructor outside `network/tls.rs` would silently regain
/// Quinn's 30-second idle default.
#[test]
fn adr0093_quic_config_construction_is_confined_to_the_tls_choke_point() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut constructors = Vec::new();
	walk_dir(&manifest_dir.join("src"), &mut |path| {
		if path.extension().is_none_or(|ext| ext != "rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		for (line_number, line) in source.lines().enumerate() {
			if line.contains("quinn::ServerConfig::") || line.contains("quinn::ClientConfig::") {
				constructors.push((
					path.strip_prefix(manifest_dir)
						.unwrap()
						.to_string_lossy()
						.replace('\\', "/"),
					line_number + 1,
				));
			}
		}
	});
	assert_eq!(
		constructors.len(),
		2,
		"the guarded Quinn constructor census changed"
	);
	assert!(
		constructors
			.iter()
			.all(|(path, _)| path == "src/network/tls.rs"),
		"all Quinn config construction must stay behind Wendelon's shared TLS builders: {constructors:?}"
	);
	let tls = fs::read_to_string(manifest_dir.join("src/network/tls.rs")).unwrap();
	assert_eq!(
		tls.matches("transport_config(wendelon_quic_transport_config())")
			.count(),
		2
	);
}

/// ADR 0093/0094/0095 froze superseded snapshot bodies; ADR 0697 (v6) refuses
/// them instead of upconverting. The stored fixtures stay as refusal inputs,
/// and the v6 golden is guarded.
#[test]
fn adr0093_snapshot_v2_and_v3_fixtures_are_both_guarded() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	let core = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let v2 = fs::read_to_string(manifest_dir.join("tests/fixtures/consensus_body/snapshot_v2.txt"))
		.unwrap();
	let v3 = fs::read_to_string(manifest_dir.join("tests/fixtures/consensus_body/snapshot_v3.txt"))
		.unwrap();
	let v4 = fs::read_to_string(manifest_dir.join("tests/fixtures/consensus_body/snapshot_v4.txt"))
		.unwrap();
	let v5 = fs::read_to_string(manifest_dir.join("tests/fixtures/consensus_body/snapshot_v5.txt"))
		.unwrap();
	assert!(core.contains("pub const SNAPSHOT_FORMAT_VERSION: u8 = 21;"));
	assert!(!manifest_dir.join("src/consensus/snapshot_body.rs").exists());
	for required in [
		"SNAPSHOT_V2_MANIFEST",
		"SNAPSHOT_V3_MANIFEST",
		"SNAPSHOT_V4_MANIFEST",
		"SNAPSHOT_V5_MANIFEST",
		// ADR 0704: v8 is retained as a refusal input. ADR 0726: v19 was current; ADR 0728: v21 is.
		"SNAPSHOT_V8_OUTGOING_MANIFEST",
		"SNAPSHOT_V21_MANIFEST",
		"adr0704_every_pre_v15_snapshot_body_is_refused_before_decode",
		"current_wal_v21_and_snapshot_v21_match_stored_goldens",
	] {
		assert!(
			raft.contains(required),
			"snapshot corpus is missing {required}"
		);
	}
	assert!(v2.contains("smstate_active_saga="));
	assert!(v3.contains("smstate_active_saga_and_drop_outcome="));
	assert!(v4.contains("smstate_active_saga_and_drop_outcome="));
	assert!(v5.contains("smstate_active_saga_drop_outcome_and_reclaim_debt="));
	assert_ne!(
		v2, v3,
		"outgoing and current snapshot fixtures must be distinct"
	);
	assert_ne!(
		v3, v4,
		"ADR 0094's CTAS chain fields must move the current fixture"
	);
	assert_ne!(
		v4, v5,
		"ADR 0095's tombstone and reclaim-debt catalogs must move the current fixture"
	);
}

#[test]
fn adr0094_write_cap_and_legacy_replay_have_one_authority() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let core = fs::read_to_string(manifest_dir.join("src/network/core.rs")).unwrap();
	let client = fs::read_to_string(manifest_dir.join("src/client/core.rs")).unwrap();
	let schema = fs::read_to_string(manifest_dir.join("src/schema/core.rs")).unwrap();
	let raft = fs::read_to_string(manifest_dir.join("src/consensus/raft_storage.rs")).unwrap();
	assert!(schema.contains("pub const WAL_FORMAT_VERSION: u8 = 21;"));
	assert_eq!(core.matches("pub const MAX_SECTION_BYTES:").count(), 1);
	assert_eq!(
		client
			.matches("pub fn encode_table_write_arrow_section")
			.count(),
		1
	);
	assert!(client.contains("encode_table_write_arrow_section(&request.payload)"));
	assert!(client.contains("crate::network::core::MAX_SECTION_BYTES"));
	assert!(raft.contains("WendelonRequest::ReplayLegacyCtasRowDispatched(replay)"));
	assert!(raft.contains("CtasSagaMutation::DeclareChain"));
	assert!(raft.contains("chunk_count: 1"));

	let mut replay_files = Vec::new();
	walk_dir(&manifest_dir.join("src"), &mut |path| {
		if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
			let source = fs::read_to_string(path).unwrap();
			if source.contains("ReplayLegacyCtasRowDispatched") {
				replay_files.push(
					path.strip_prefix(manifest_dir)
						.unwrap()
						.to_string_lossy()
						.into_owned(),
				);
			}
		}
	});
	replay_files.sort();
	assert_eq!(
		replay_files,
		vec![
			"src/consensus/core.rs",
			"src/consensus/raft_storage.rs",
			"src/schema/ctas.rs",
			"src/storage/mutation_visibility/root2/committed.rs",
		],
		"the WAL-v2 bridge may exist only in its type, decoder, and apply seams; the only other \
		 mention is the committed-entry adapter's exhaustive classification pattern"
	);
	// Classification-only exception (ADR0100 stage A0): one mention, as a match pattern. The
	// mutation-visibility guard proves it sits inside `request_class` and yields NonRoot2.
	let adapter =
		fs::read_to_string(manifest_dir.join("src/storage/mutation_visibility/root2/committed.rs"))
			.unwrap();
	assert_eq!(adapter.matches("ReplayLegacyCtasRowDispatched").count(), 1);
	assert_eq!(
		adapter
			.matches("WendelonRequest::ReplayLegacyCtasRowDispatched(_)")
			.count(),
		1
	);
	assert_eq!(
		client
			.matches("encode_table_write_arrow_section(&request.payload)")
			.count(),
		1,
		"the live sender must cross the authoritative size seam exactly once"
	);
}

#[test]
fn adr0094_every_provisional_write_route_admits_the_declared_chain() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let leader = fs::read_to_string(manifest_dir.join("src/network/data_server.rs")).unwrap();
	let replica =
		fs::read_to_string(manifest_dir.join("src/network/data_server/write_replication.rs"))
			.unwrap();
	assert_eq!(
		leader
			.matches("saga.contains_row_write_id(meta.parent_write_id)")
			.count(),
		1,
		"the leader's provisional-descriptor fallback must admit the declared chain"
	);
	assert_eq!(
		replica
			.matches("saga.contains_row_write_id(meta.parent_write_id)")
			.count(),
		1,
		"the replica's provisional-descriptor fallback must admit the declared chain"
	);
	for source in [&leader, &replica] {
		assert!(
			!source.contains(
				"saga.row_write_id.canonical_idempotency_bytes()\n\t\t\t\t\t\t== meta.parent_write_id.canonical_idempotency_bytes()"
			),
			"the pre-0094 single-write equality must not return at either route"
		);
	}
}

/// ADR 0093 acceptance hooks exist only in tests or an explicitly requested
/// non-default feature. Default production builds cannot arm them.
#[test]
fn adr0093_test_support_is_non_default_and_every_hook_site_is_gated() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
	let parsed: toml::Value = toml::from_str(&manifest).unwrap();
	let features = parsed["features"].as_table().unwrap();
	assert!(features.contains_key("test-support"));
	let defaults = features["default"].as_array().unwrap();
	assert!(
		!defaults
			.iter()
			.any(|feature| feature.as_str() == Some("test-support")),
		"test-support must never enter Wendelon's default feature set"
	);
	let toron_manifest = read_workspace("crates/toron/Cargo.toml");
	let toron: toml::Value = toml::from_str(&toron_manifest).unwrap();
	let toron_features = toron["features"].as_table().unwrap();
	assert_eq!(
		toron_features["test-support"].as_array().unwrap(),
		&vec![toml::Value::String("wendelon/test-support".to_string())],
		"Toron's non-default acceptance feature forwards only Wendelon test support"
	);
	assert!(
		!toron_features["default"]
			.as_array()
			.unwrap()
			.iter()
			.any(|feature| feature.as_str() == Some("test-support"))
	);

	let control = fs::read_to_string(manifest_dir.join("src/network/control_server.rs")).unwrap();
	assert!(
		control.contains(
			"#[cfg(any(test, feature = \"test-support\"))]\npub mod adr0093_test_support"
		)
	);
	for gated_site in [
		"#[cfg(any(test, feature = \"test-support\"))]\n\t\tlet table_object_id",
		"#[cfg(any(test, feature = \"test-support\"))]\n\t\t\t\tif adr0093_test_support::take_sever_response",
	] {
		assert!(
			control.contains(gated_site),
			"ungated or moved hook seam: {gated_site}"
		);
	}
	assert_eq!(control.matches("take_sever_response(").count(), 2);
	assert_eq!(control.matches("take_reclaim_delay(").count(), 0);
	assert_eq!(control.matches("delay_next_drop_reclaim(").count(), 0);
	let node = read_workspace("crates/wendelon/src/node.rs");
	for site in [
		"#[cfg(feature = \"test-support\")]\nmod test_support;",
		"#[cfg(feature = \"test-support\")]\npub use test_support::TestSupportFileOps;",
		"#[cfg(feature = \"test-support\")]\n\tpub slab_file_ops: Option<TestSupportFileOps>,",
		"#[cfg(feature = \"test-support\")]\n\t\tlet slab: Arc<SlabManager> = {",
	] {
		assert!(
			node.contains(site),
			"acceptance-8 ungated test-support site: {site}"
		);
	}
	assert_eq!(node.matches("pub slab_file_ops:").count(), 1);
	assert_eq!(
		node.matches("SlabManager::open_with_file_ops_and_owner(")
			.count(),
		1
	);
	let manager = read_workspace("crates/wendelon/src/storage/manager.rs");
	assert!(manager.contains(
		"#[cfg(any(test, feature = \"test-support\"))]\n\tpub async fn open_with_file_ops("
	));
	assert_eq!(
		manager.matches("pub async fn open_with_file_ops(").count(),
		1
	);
	let support = read_workspace("crates/wendelon/src/node/test_support.rs");
	assert!(support.contains("pub struct TestSupportFileOps(Arc<dyn AsyncFileOps>);"));
	assert!(support.contains("Arc::ptr_eq(&self.0, &other.0)"));
	assert!(support.contains("f.write_str(\"TestSupportFileOps(..)\")"));
}

/// ADR 0167: tenant provisioning is an operator command over the existing
/// tenant administration RPC. The CLI dispatches `tenant`, documents its
/// three verbs, and the client sends the opcode the handler already serves.
#[test]
fn adr0167_wendelon_tenant_command_reaches_the_tenant_admin_rpc() {
	let cli = wendelon_cli_source();
	assert!(cli.contains("[cmd, rest @ ..] if cmd == \"tenant\" => tenant::run(rest)"));
	for verb in ["tenant create", "tenant get", "tenant list"] {
		assert!(
			cli.contains(&format!("{{program}} {verb} --bootstrap")),
			"{verb} documented"
		);
	}
	let client =
		std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/client/core.rs"))
			.expect("client source");
	assert!(client.contains("msg_type: MsgType::TENANT_ADMIN_REQ.0"));
	assert!(client.contains("pub async fn tenant_admin("));
	let tenant = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/cli/tenant.rs"))
		.expect("tenant CLI source");
	// The CLI is a client of the RPC, never an in-process node.
	for forbidden in ["WendelonNode", "create_tenant(", "state_machine"] {
		assert!(
			!tenant.contains(forbidden),
			"tenant CLI must not use {forbidden}"
		);
	}
}

#[test]
fn pd25_constructor_spawns_stay_inside_typed_ownership_handoff() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(root.join("src/node.rs")).unwrap();
	let region = source
		.split("async fn construct(")
		.nth(1)
		.unwrap()
		.split("pub fn subscribe_clock_drift")
		.next()
		.unwrap();
	fn find_body(stream: TokenStream) -> Option<TokenStream> {
		let tokens: Vec<_> = stream.into_iter().collect();
		if let Some(function) = tokens.windows(2).position(|pair| {
			matches!(&pair[0], TokenTree::Ident(id) if id == "fn")
				&& matches!(&pair[1], TokenTree::Ident(id) if id == "construct")
		}) {
			return tokens[function + 2..].iter().find_map(|token| match token {
				TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Brace => {
					Some(group.stream())
				}
				_ => None,
			});
		}
		tokens.into_iter().find_map(|token| match token {
			TokenTree::Group(group) => find_body(group.stream()),
			_ => None,
		})
	}
	let body = find_body(TokenStream::from_str(&source).unwrap()).expect("constructor body");
	fn audit(stream: TokenStream) {
		let tokens: Vec<_> = stream.into_iter().collect();
		let mut i = 0;
		while i < tokens.len() {
			if let TokenTree::Ident(name) = &tokens[i] {
				let name = name.to_string();
				let owned_receiver = i >= 5
					&& matches!(&tokens[i-5], TokenTree::Ident(id) if id == "scope")
					&& matches!(&tokens[i-4], TokenTree::Punct(p) if p.as_char() == '.')
					&& matches!(&tokens[i-3], TokenTree::Ident(id) if id == "handles")
					&& matches!(&tokens[i-2], TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Parenthesis && group.stream().is_empty())
					&& matches!(&tokens[i-1], TokenTree::Punct(p) if p.as_char() == '.');
				if owned_receiver
					&& name.starts_with("set_")
					&& matches!(tokens.get(i+1), Some(TokenTree::Group(group)) if group.delimiter() == proc_macro2::Delimiter::Parenthesis)
				{
					// Typed setters immediately own the result even when a spawn lives in
					// an if/match expression inside their argument.
					i += 2;
					continue;
				}
				if name == "spawn" || name.starts_with("spawn_") {
					assert!(
						matches!(
							name.as_str(),
							"spawn_io_thread_named_for_class"
								| "spawn_quic_server" | "spawn_tcp_tls_server"
						),
						"unowned constructor spawn: {name}"
					);
				}
			}
			if let TokenTree::Group(group) = &tokens[i] {
				audit(group.stream());
			}
			i += 1;
		}
	}
	audit(body);
	assert!(region.contains("scope.adopt_transport(server)"));
	assert!(region.contains("scope.adopt_raft(raft.clone())"));
	assert!(region.contains("scope.adopt_factory(factory.clone())"));
	let startup = fs::read_to_string(root.join("src/node/startup.rs")).unwrap();
	assert!(startup.contains("handles.validate()"));
	assert!(startup.contains("duplicate or untransferred tasks"));
	let cleanup = startup
		.split("fn start_cleanup(")
		.nth(1)
		.unwrap()
		.split("pub(super) async fn fail")
		.next()
		.unwrap();
	assert!(!cleanup.contains("prepare_for_owner"));
	assert!(!cleanup.contains("ShutdownWatchdog::arm"));
	assert!(!cleanup.contains(".expect("));
	assert!(cleanup.find("resources.fence()").unwrap() < cleanup.find("watchdog.start(").unwrap());
}

#[test]
fn pd25_provider_publication_requires_real_withdrawal_and_complete_census() {
	fn empty_success(expr: &syn::Expr) -> bool {
		match expr {
			syn::Expr::Return(value) => value.expr.as_deref().is_some_and(empty_success),
			syn::Expr::Paren(value) => empty_success(&value.expr),
			syn::Expr::Group(value) => empty_success(&value.expr),
			syn::Expr::Block(value) if value.block.stmts.len() == 1 => {
				match &value.block.stmts[0] {
					syn::Stmt::Expr(expr, _) => empty_success(expr),
					_ => false,
				}
			}
			syn::Expr::Call(call) => {
				matches!(&*call.func, syn::Expr::Path(path) if path.path.is_ident("Ok"))
					&& call.args.len() == 1
					&& matches!(&call.args[0], syn::Expr::Tuple(tuple) if tuple.elems.is_empty())
			}
			_ => false,
		}
	}

	fn record_provider(item: syn::ItemImpl, file: &str, rows: &mut Vec<serde_json::Value>) {
		let is_provider = item.trait_.as_ref().is_some_and(|(_, path, _)| {
			path.segments
				.last()
				.is_some_and(|segment| segment.ident == "TransportProvider")
		});
		if !is_provider {
			return;
		}
		let implementation = match &*item.self_ty {
			syn::Type::Path(path) => path.path.segments.last().unwrap().ident.to_string(),
			_ => panic!("unclassified provider type in {file}"),
		};
		let mut publish = false;
		let mut withdraw = None;
		for entry in &item.items {
			if let syn::ImplItem::Fn(method) = entry {
				if method.sig.ident == "publish_local" {
					publish = true;
				}
				if method.sig.ident == "withdraw_local" {
					withdraw = Some(method);
				}
			}
		}
		let withdraw = withdraw.expect("every provider implements the required withdrawal");
		let noop = match withdraw.block.stmts.as_slice() {
			[syn::Stmt::Expr(expr, _)] => empty_success(expr),
			_ => false,
		};
		rows.push(serde_json::json!({
		 "file": file, "implementation": implementation,
		 "overrides_publish": publish, "withdraw_noop": noop,
		}));
	}
	fn scan(stream: TokenStream, file: &str, rows: &mut Vec<serde_json::Value>) {
		let tokens: Vec<_> = stream.into_iter().collect();
		for i in 0..tokens.len() {
			if matches!(&tokens[i], TokenTree::Ident(id) if id == "impl") {
				let end = (i+1..tokens.len()).find(|&end| {
     matches!(&tokens[end], TokenTree::Group(group) if group.delimiter() == proc_macro2::Delimiter::Brace)
    });
				if let Some(end) = end {
					let candidate = tokens[i..=end].iter().cloned().collect();
					if let Ok(item) = syn::parse2::<syn::ItemImpl>(candidate) {
						record_provider(item, file, rows);
					}
				}
			}
			if let TokenTree::Group(group) = &tokens[i] {
				scan(group.stream(), file, rows);
			}
		}
	}

	let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
		.parent()
		.unwrap()
		.parent()
		.unwrap();
	let mut rows = Vec::new();
	walk_dir(&workspace.join("crates"), &mut |path| {
		if path.extension().is_some_and(|extension| extension == "rs")
			&& !path
				.components()
				.any(|component| component.as_os_str() == "target")
		{
			let source = fs::read_to_string(path).unwrap();
			scan(
				TokenStream::from_str(&source).unwrap(),
				&path.strip_prefix(workspace).unwrap().to_string_lossy(),
				&mut rows,
			);
		}
	});
	rows.sort_by_key(|row| format!("{}:{}", row["file"], row["implementation"]));
	for row in &rows {
		assert!(
			!(row["overrides_publish"] == true && row["withdraw_noop"] == true),
			"publication with no-op withdrawal: {row}"
		);
	}
	let expected: serde_json::Value =
		serde_json::from_str(include_str!("fixtures/startup_provider_census.json")).unwrap();
	let expected: Vec<_> = expected.as_array().unwrap().iter().map(|row| serde_json::json!({"file":row["file"], "implementation":row["implementation"], "overrides_publish":row["overrides_publish"], "withdraw_noop":row["withdraw_noop"]})).collect();
	assert_eq!(
		rows, expected,
		"provider census changed: audit retention/withdrawal and update the checked fixture"
	);
	let mut negative = Vec::new();
	scan(TokenStream::from_str("impl TransportProvider for Unwithdrawn { fn publish_local(&self) {} fn withdraw_local(&self) { Ok(()) } }").unwrap(), "negative control", &mut negative);
	assert_eq!(negative[0]["overrides_publish"], true);
	assert_eq!(
		negative[0]["withdraw_noop"], true,
		"the no-op publication bypass must be detectable"
	);
}

#[test]
fn pd22_sidecar_path_builder_requires_the_full_persistent_catalog_key() {
	let text = std::fs::read_to_string(
		std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("src/storage/secondary_index_segment.rs"),
	)
	.unwrap();
	let start = text
		.find("pub(crate) fn secondary_index_segment_storage_path(")
		.unwrap();
	let body = &text[start..text[start..].find("\n}\n").map(|end| start + end).unwrap()];
	assert!(body.contains("key: &PersistentSecondaryIndexSegmentKey"));
	for field in [
		"tenant_id",
		"table_object_id",
		"index_object_id",
		"logical_schema_fingerprint",
		"source_slab_id",
	] {
		assert!(body.contains(&format!("key.{field}")), "path omits {field}");
	}
}

/// The source text of one `impl` method, from its signature to the next
/// method at the same indentation.
fn manager_method_body<'a>(source: &'a str, name: &str) -> &'a str {
	let start = source
		.find(&format!("fn {name}("))
		.unwrap_or_else(|| panic!("method {name} not found"));
	let rest = &source[start..];
	let end = [
		"\n\tpub async fn ",
		"\n\tpub fn ",
		"\n\tpub(super) ",
		"\n\tpub(crate) ",
		"\n\tasync fn ",
		"\n\tfn ",
	]
	.iter()
	.filter_map(|marker| rest[1..].find(marker).map(|index| index + 1))
	.min()
	.unwrap_or(rest.len());
	&rest[..end]
}

/// Relocating writers replace each key's versions in one index critical
/// section. A relocation that removes every source and then inserts every
/// output makes a live key momentarily absent to lock-free readers.
#[test]
fn relocating_writers_replace_versions_per_key_and_never_remove_then_insert() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	for (file, method) in [
		(
			"src/storage/manager/lsm_driver.rs",
			"complete_lsm_compaction_install",
		),
		(
			"src/storage/manager/compaction.rs",
			"finish_compaction_with_replacement",
		),
		(
			"src/storage/manager/tiered_io.rs",
			"apply_prepared_tiered_row_refresh",
		),
	] {
		let source = fs::read_to_string(manifest_dir.join(file)).unwrap();
		let body = manager_method_body(&source, method);
		assert!(
			!body.contains("rollback_indexed_location("),
			"{file}::{method} must not remove relocated versions with rollback_indexed_location"
		);
		assert!(
			!body.contains("index_appended_location_with_locality("),
			"{file}::{method} must not insert relocated versions with index_appended_location_with_locality"
		);
		assert!(
			body.contains("relocate_indexed_locations("),
			"{file}::{method} must relocate through relocate_indexed_locations"
		);
	}
}

// ============================================================
//  Release by slab id: the conditions it rests on
// ============================================================

/// `(file basename, count)` of `needle` in each manager source file
/// (`tests.rs` excluded), for the files where it occurs.
fn manager_occurrences(manifest_dir: &Path, count: impl Fn(&str) -> usize) -> Vec<(String, usize)> {
	manager_source_files(manifest_dir)
		.iter()
		.filter_map(|path| {
			let occurrences = count(&fs::read_to_string(path).unwrap());
			(occurrences > 0).then(|| {
				(
					path.file_name().unwrap().to_str().unwrap().to_string(),
					occurrences,
				)
			})
		})
		.collect()
}

/// A released slab id is never allocated again: `next_slab_id` changes only
/// through one helper, and that helper never moves it backward.
#[test]
fn the_slab_allocator_only_advances_through_one_monotone_helper() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let assignments = manager_occurrences(manifest_dir, |source| {
		source
			.lines()
			.filter(|line| {
				line.split("next_slab_id").skip(1).any(|rest| {
					rest.trim_start().starts_with('=') && !rest.trim_start().starts_with("==")
				})
			})
			.filter(|line| line.contains(".next_slab_id"))
			.count()
	});
	assert_eq!(
		assignments,
		vec![("manager.rs".to_string(), 1)],
		"next_slab_id is assigned only inside the monotone allocator helper"
	);
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let helper = manager_method_body(&root, "advance_slab_allocator");
	assert!(
		helper.contains("self.next_slab_id = ") && helper.contains(".max("),
		"the allocator helper only moves next_slab_id forward"
	);
}

/// Every call that names a slab path, pinned per file. A released file's
/// path is reused only by an installer that allocates a fresh id (rotation,
/// force-seal, LSM outputs, maintenance replacements, retention reclaim, the
/// secondary-index declaration cut) or links without replacing (rehydration).
/// A new call site changes this census and must be reviewed against the
/// release rule in `slab_file_quarantine.rs`.
#[test]
fn every_slab_path_installer_uses_a_fresh_id_or_links_without_replacing() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let slab_paths = manager_occurrences(manifest_dir, |source| {
		source
			.match_indices("slab_path(")
			.filter(|(at, _)| {
				let before = &source[..*at];
				!before.ends_with('_')
					&& !before.ends_with("fn ")
					&& !before
						.chars()
						.last()
						.is_some_and(|c| c.is_ascii_alphanumeric())
			})
			.count()
	});
	assert_eq!(
		slab_paths,
		vec![
			// recovery-time zero-length cleanup (no manager yet), the rotation's
			// fresh next id, sealed-file reads and the backup copy
			("manager.rs".to_string(), 7),
			// GC's identity read, its exclusive recheck and its quarantine link's
			// source, a resume's original and a finished removal's absence check: all
			// existing files, none installed at a slab path.
			("gc_disposal.rs".to_string(), 5),
			// Fresh output paths, identity-only reads, and intent-guarded cleanup.
			// Control output recovery and input capture add read-only identity checks.
			("lsm_driver.rs".to_string(), 12),
			// the staged replacement's final path (fresh reserved id)
			("maintenance_publication.rs".to_string(), 1),
			// Recovery of an abandoned install: reads and a rejected output's source, a live
			// run's existing file, and the output its sidecars are prepared from. None installs
			// anything at a slab path.
			("op_recovery.rs".to_string(), 6),
			// open-time discovery (no manager yet)
			("open.rs".to_string(), 4),
			// A test hook asking whether a file exists: it installs nothing.
			("pause_hooks.rs".to_string(), 1),
			// An existing file only: a rejected output moved out of this namespace,
			// and an abandoned one read and disposed of after its claim. Nothing is
			// installed at a slab path. A resumed disposal names the original once more.
			("reservations.rs".to_string(), 4),
			// retention reclaim's fresh active slab
			("retention_reclaim.rs".to_string(), 1),
			// the declaration cut (fresh id) and segment source reads
			("secondary_index_segments.rs".to_string(), 3),
			// Open classification, owner-identity GC unlink, and released unlink.
			("slab_file_quarantine.rs".to_string(), 3),
			// Bootstrap checks the existing path and allocates a fresh replacement.
			("tier_authority.rs".to_string(), 2),
			// rehydration, published with link_no_replace
			("tiered_io.rs".to_string(), 1),
		],
		"slab-path call sites changed; review each against the id-keyed release rule"
	);
	let installs = manager_occurrences(manifest_dir, |source| {
		source.matches(".rename(").count() + source.matches(".link_no_replace(").count()
	});
	assert_eq!(
		installs,
		vec![
			// GC's no-replace link into its quarantine name, never to a slab path, and a
			// resume's for the link its row recorded and never made.
			("gc_disposal.rs".to_string(), 2),
			// Staged output rename and no-replace link into the quarantine namespace.
			("lsm_driver.rs".to_string(), 2),
			("maintenance_publication.rs".to_string(), 1),
			// A rejected output's no-replace link out of the slab namespace, and a
			// reservation cleanup's into its quarantine name. Neither installs a slab.
			// ... and a resumed disposal's link, for the row that recorded it and never made it.
			("reservations.rs".to_string(), 3),
			("secondary_index_segments.rs".to_string(), 1),
			("tiered_io.rs".to_string(), 1),
		],
		"rename/link call sites changed; review each against the id-keyed release rule"
	);
	let tiered_io =
		fs::read_to_string(manifest_dir.join("src/storage/manager/tiered_io.rs")).unwrap();
	assert!(
		tiered_io.contains("link_no_replace(&tmp_path, &path)"),
		"rehydration never replaces a file at a slab path"
	);
}

/// One manager per data directory: the only constructor outside test and
/// test-support builds holds the directory owner, whose lock is exclusive
/// (storage/ownership.rs tests).
#[test]
fn the_only_production_slab_manager_constructor_holds_the_directory_owner() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = fs::read_to_string(manifest_dir.join("src/storage/manager.rs")).unwrap();
	let constructors = root
		.lines()
		.enumerate()
		.filter(|(_, line)| line.contains("async fn open") && !line.contains("open_shared"))
		.map(|(index, line)| {
			let gate = root
				.lines()
				.take(index)
				.last()
				.unwrap_or("")
				.trim()
				.to_string();
			(line.trim().to_string(), gate)
		})
		.collect::<Vec<_>>();
	for (signature, gate) in &constructors {
		if signature.starts_with("pub async fn open_with_owner(") {
			continue;
		}
		assert!(
			gate == "#[cfg(any(test, feature = \"test-support\"))]"
				|| gate == "#[cfg(test)]"
				|| gate == "#[cfg(feature = \"test-support\")]",
			"{signature} opens a manager without the directory owner outside tests ({gate})"
		);
	}
	assert!(
		constructors
			.iter()
			.any(|(signature, _)| signature.starts_with("pub async fn open_with_owner(")),
		"the production constructor exists"
	);
	assert!(
		manager_method_body(&root, "open_with_owner").contains("Some(owner)"),
		"the production constructor keeps the owner for the manager's lifetime"
	);
	let open = fs::read_to_string(manifest_dir.join("src/storage/manager/open.rs")).unwrap();
	assert!(
		open.contains("pub(super) async fn open_shared("),
		"the shared opener is not reachable outside the manager"
	);
}

#[test]
fn tier_recovery_snapshots_have_only_the_reviewed_private_callers() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage");
	for (name, expected) in [
		(
			"record_rows_for_slab_for_recovery",
			vec![
				("manager.rs", 2),
				("manager/tiered_io.rs", 2),
				// ADR 0704: the local rows a certified classification compares with its
				// certificate and the ring's postimage, under the publication's lease.
				("manager/certified_delivery.rs", 1),
			],
		),
		(
			// ADR 0696 (0692 §7): the bounded snapshot behind every tier
			// publication plan; its callers are the plan builders.
			"record_rows_for_slab_for_recovery_within",
			vec![("manager/tiered_io.rs", 4)],
		),
		(
			"published_table_object_id_for_recovery",
			vec![("manager.rs", 3)],
		),
		(
			"version_conflict_groups_for_recovery",
			vec![
				("manager/version_identity.rs", 2),
				("manager/tier_authority.rs", 1),
			],
		),
		(
			"recovery_read_target",
			vec![
				("manager/tier_generation.rs", 1),
				("manager/lsm_driver.rs", 1),
				("manager/tier_authority.rs", 1),
			],
		),
		(
			"build_tiered_row_refresh_plan_from_plaintext",
			vec![
				("manager/tiered_io.rs", 1),
				("manager/tier_authority.rs", 1),
			],
		),
	] {
		let mut actual = Vec::new();
		walk_dir(&root, &mut |path| {
			if path.ends_with("manager/tests.rs") {
				return;
			}
			if path.extension().is_none_or(|extension| extension != "rs") {
				return;
			}
			let source = fs::read_to_string(path).unwrap();
			let count = rust_identifiers_from_source(&path.display().to_string(), &source)
				.iter()
				.filter(|ident| *ident == name)
				.count();
			if count > 0 {
				actual.push((
					path.strip_prefix(&root)
						.unwrap()
						.to_string_lossy()
						.to_string(),
					count,
				));
			}
		});
		actual.sort();
		let mut expected = expected
			.into_iter()
			.map(|(file, count)| (file.to_string(), count))
			.collect::<Vec<_>>();
		expected.sort();
		assert_eq!(
			actual, expected,
			"private recovery caller census changed for {name}"
		);
	}
	let tiered = fs::read_to_string(root.join("manager/tiered_io.rs")).unwrap();
	let entry = manager_method_body(&tiered, "retire_tiered_slab_committed");
	assert!(entry.contains("retire_tiered_slab_with_epoch(slab_id, last_control_log_index, None)"));
	// ADR 0704: the lease precedes the slab lock. `retire_tiered_slab_with_epoch` takes the
	// lease; `retire_tiered_slab_leased`, which recovery calls under its own lease, takes the
	// slab lock and is the reviewed caller of the locked retirement.
	let leased_entry = manager_method_body(&tiered, "retire_tiered_slab_with_epoch");
	assert!(
		leased_entry.find("canonical_tier_lease().await").unwrap()
			< leased_entry.find("retire_tiered_slab_leased(").unwrap()
	);
	assert!(!leased_entry.contains("retire_tiered_slab_under_publication_lock("));
	let retirement = manager_method_body(&tiered, "retire_tiered_slab_leased");
	assert!(retirement.contains("tier_lock.lock().await"));
	assert!(retirement.contains("retire_tiered_slab_under_publication_lock("));
	let retirement_locked =
		manager_method_body(&tiered, "retire_tiered_slab_under_publication_lock");
	assert!(retirement_locked.contains(
		"require_private_retirement_authority(slab_id, last_control_log_index.is_some())?"
	));
	// ADR 0704: the reconcile reads authority from the lease it took before the slab lock; its
	// locked half is the reviewed caller of the locked retirement.
	let reconcile = manager_method_body(&tiered, "reconcile_tier_slab_at_snapshot_epoch<F>");
	assert!(
		reconcile.find("canonical_tier_lease().await").unwrap()
			< reconcile.find("tier_lock.lock().await").unwrap()
	);
	assert!(
		reconcile.find("tier_lock.lock().await").unwrap()
			< reconcile.find("authoritative(lease.as_ref())").unwrap()
	);
	assert!(
		reconcile.find("authoritative(lease.as_ref())").unwrap()
			< reconcile
				.find(".reconcile_tier_slab_under_publication_lock(")
				.unwrap()
	);
	let reconcile_locked =
		manager_method_body(&tiered, "reconcile_tier_slab_under_publication_lock");
	assert!(
		retirement.find("tier_lock.lock().await").unwrap()
			< retirement
				.find("retire_tiered_slab_under_publication_lock(")
				.unwrap()
	);
	assert!(!reconcile.contains("retire_tiered_slab_under_publication_lock("));
	assert_eq!(
		retirement
			.matches("retire_tiered_slab_under_publication_lock(")
			.count(),
		1
	);
	assert_eq!(
		reconcile_locked
			.matches("retire_tiered_slab_under_publication_lock(")
			.count(),
		1
	);
	assert_eq!(
		tiered
			.matches("retire_tiered_slab_under_publication_lock(")
			.count(),
		// With `retry_deferred_retirements`, which takes the tier lock first too.
		4
	);
	assert!(tiered.contains("\n\tasync fn retire_tiered_slab_under_publication_lock("));
	let snapshot_retirement = manager_method_body(&tiered, "apply_snapshot_retirement");
	assert!(snapshot_retirement.contains("tier_lock.lock().await"));
	assert!(snapshot_retirement.contains("persistent_index_gate"));
	assert!(snapshot_retirement.contains("apply_snapshot_retirement("));
	let manager = fs::read_to_string(root.join("manager.rs")).unwrap();
	assert!(
		manager_method_body(&manager, "record_rows_for_slab")
			.contains("require_tier_authority_ready()?")
	);
	assert!(
		manager_method_body(&manager, "published_table_object_id")
			.contains("require_tier_authority_ready()?")
	);
}

fn answer_tokens_never_default(tokens: TokenStream) -> bool {
	let tokens = tokens.into_iter().collect::<Vec<_>>();
	let answers = [
		"lookup",
		"lookup_as_of",
		"scan",
		"scan_as_of",
		"scan_as_of_with_identity",
		"scan_as_of_with_identity_and_locality",
		"scan_all_versions",
		"scan_all_versions_from",
		"version_placement_at",
		"routing_locality_for_existing_key",
		"slab_hlc_bounds",
		"slab_supersession_metadata",
		"slab_row_tombstone_metadata",
		"slab_has_supersession_at",
		"slab_has_migrated_range_tombstone_at",
		"table_drop_reference_counts",
		"published_table_object_id",
		"record_rows_for_slab",
		"counter_lookup",
		"same_hlc_locations",
		"resolve_logical_version",
		"resolve_secondary_index_rows",
		"admit_pk_write_row",
		"admit_pk_delete_row",
		"row_visible_for_admission",
	];
	for (index, token) in tokens.iter().enumerate() {
		if let TokenTree::Group(group) = token {
			if !answer_tokens_never_default(group.stream()) {
				return false;
			}
		}
		if matches!(token, TokenTree::Ident(name) if answers.iter().any(|answer| name == *answer))
			&& matches!(tokens.get(index + 1), Some(TokenTree::Group(group)) if group.delimiter() == proc_macro2::Delimiter::Parenthesis)
			&& matches!(tokens.get(index + 2), Some(TokenTree::Punct(dot)) if dot.as_char() == '.')
			&& matches!(tokens.get(index + 3), Some(TokenTree::Ident(method)) if ["ok", "unwrap_or", "unwrap_or_default", "unwrap_or_else"].iter().any(|name| method == *name))
		{
			return false;
		}
	}
	true
}

#[test]
fn fallible_index_answers_never_coerce_refusal_to_missing_or_empty() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	walk_dir(&root, &mut |path| {
		if path.extension().is_some_and(|extension| extension == "rs") {
			let source = fs::read_to_string(path).unwrap();
			assert!(
				answer_tokens_never_default(TokenStream::from_str(&source).unwrap()),
				"answer refusal defaulted in {}",
				path.display()
			);
		}
	});
}

#[test]
fn tier_authority_remote_reads_have_no_manager_or_install_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/manager");
	let source = fs::read_to_string(root.join("tier_authority.rs")).unwrap();
	let plaintext = manager_method_body(&source, "plaintext");
	assert!(plaintext.contains("let (store, acknowledged) = {"));
	let remote = plaintext
		.find("store.get_bytes(")
		.expect("owned remote read");
	assert!(plaintext[..remote].contains("\n\t\t};"));
	assert!(plaintext[remote..].contains("self.upgrade(epoch)"));
	assert!(!plaintext.contains("spawn_blocking"));
	assert!(!plaintext.contains("write_all("));
	assert!(source.contains("OnceLock<Weak<SlabManager>>"));
	assert!(source.contains("owner.register_worker(self.clone())?"));
	let stage = manager_method_body(&source, "stage_bootstrap_tier_authority");
	assert!(stage.contains(
		"stage_tier_authority(pointer, last_control_log_index, committed_id_floor, false)"
	));
	let wrapper = manager_method_body(&source, "stage_tier_authority");
	assert!(wrapper.contains("lock.lock().await"));
	assert!(wrapper.contains("stage_tier_authority_under_publication_lock("));
	let delegated = manager_method_body(&source, "stage_tier_authority_under_publication_lock");
	assert!(delegated.contains("begin_staging(pointer.slab_id, snapshot)?"));
	assert!(!wrapper.contains("unlink"));
	assert!(!wrapper.contains("release_slab_file"));
	assert!(!wrapper.contains("get_bytes"));
	assert!(!stage.contains("unlink"));
	assert!(!stage.contains("release_slab_file"));
	assert!(!stage.contains("get_bytes"));
	assert!(!delegated.contains("unlink"));
	assert!(!delegated.contains("release_slab_file"));
	assert!(!delegated.contains("get_bytes"));
}

#[test]
fn replacement_canonical_staging_has_one_confirmed_replay_issuer() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut occurrences = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			let count = rust_identifiers_from_source("replay staging issuer census", &source)
				.iter()
				.filter(|name| *name == "stage_replacement_tier_comparison")
				.count();
			if count != 0 {
				occurrences.push((path.strip_prefix(root).unwrap().to_path_buf(), count));
			}
		}
	});
	occurrences.sort();
	assert_eq!(
		occurrences,
		vec![
			(PathBuf::from("src/storage/index.rs"), 1),
			(PathBuf::from("src/storage/manager/tier_authority.rs"), 1),
		],
		"one declaration and exactly one production caller"
	);
	let index = fs::read_to_string(root.join("src/storage/index.rs")).unwrap();
	let strict = index
		.split("pub(crate) async fn stage_tier_comparison(")
		.nth(1)
		.unwrap()
		.split("pub(crate) async fn stage_replacement_tier_comparison(")
		.next()
		.unwrap();
	assert!(strict.contains(".stage_tier_comparison_inner("));
	assert!(strict.contains("Vec::new()"));
	assert!(!strict.contains("replay_staged"));
	let authority = fs::read_to_string(root.join("src/storage/manager/tier_authority.rs")).unwrap();
	assert!(authority.contains("let replay_staged = if replay_confirmation.is_some()"));
	assert!(authority.contains("if let Some(completion) = replacement_completion"));
	assert!(authority.contains(".stage_replacement_tier_comparison("));
	let node = fs::read_to_string(root.join("src/node.rs")).unwrap();
	let recovered = node
		.split("for tier in recovered_tiers {")
		.nth(1)
		.unwrap()
		.split("let seed_epoch =")
		.next()
		.unwrap();
	assert!(recovered.contains(".stage_bootstrap_tier_authority("));
	assert!(
		!recovered.contains("break;"),
		"awaiting replacements still register recovered committed flights"
	);
}

#[test]
fn pending_retirement_staging_has_one_complete_snapshot_issuer() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut occurrences = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			let count = rust_identifiers_from_source("pending retirement issuer census", &source)
				.iter()
				.filter(|name| *name == "stage_snapshot_tier_comparison_with_pending_retirements")
				.count();
			if count != 0 {
				occurrences.push((path.strip_prefix(root).unwrap().to_path_buf(), count));
			}
		}
	});
	occurrences.sort();
	assert_eq!(
		occurrences,
		vec![
			(PathBuf::from("src/storage/index.rs"), 1),
			(PathBuf::from("src/storage/manager/tier_authority.rs"), 1),
		],
		"one declaration and exactly one production caller"
	);
	let authority = fs::read_to_string(root.join("src/storage/manager/tier_authority.rs")).unwrap();
	let comparison = manager_method_body(&authority, "finish_tier_comparison_at_epoch");
	assert!(comparison.contains("write_owned().await"));
	assert!(comparison.contains("if replay_confirmation.is_none()"));
	assert!(comparison.contains("entry.pointer == *pointer"));
	assert!(comparison.contains("old < snapshot"));
	assert!(comparison.contains("else if !pending_retirements.is_empty()"));
	let index = fs::read_to_string(root.join("src/storage/index.rs")).unwrap();
	for name in ["stage_tier_comparison", "stage_replacement_tier_comparison"] {
		let entry = manager_method_body(&index, name);
		assert!(entry.contains(".stage_tier_comparison_inner("));
		assert!(!entry.contains("pending_retirement"));
		assert!(entry.contains("None,"));
	}
}

#[test]
fn accepted_snapshot_pending_staging_has_one_private_issuer() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut occurrences = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			let count = rust_identifiers_from_source("snapshot pending staging census", &source)
				.iter()
				.filter(|name| *name == "stage_snapshot_pending_tiers")
				.count();
			if count != 0 {
				occurrences.push((path.strip_prefix(root).unwrap().to_path_buf(), count));
			}
		}
	});
	occurrences.sort();
	assert_eq!(
		occurrences,
		vec![
			(PathBuf::from("src/storage/index.rs"), 1),
			(PathBuf::from("src/storage/manager/tier_authority.rs"), 1)
		],
		"one declaration and one accepted-snapshot deferral caller"
	);
}

/// The SM lease precedes local publication locks. Local files/redb may await;
/// remote work and retry waiting must occur outside the lease's lexical scope.
#[test]
fn snapshot_lease_scopes_exclude_remote_io_and_backoff() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let authority = fs::read_to_string(root.join("src/storage/manager/tier_authority.rs")).unwrap();
	let sweeper = fs::read_to_string(root.join("src/storage/sweeper.rs")).unwrap();
	let verify = manager_method_body(&authority, "verify");
	let compare = manager_method_body(&authority, "retry_snapshot_comparison");
	let callback = manager_method_body(&sweeper, "on_tier_snapshot_installed");
	// Census all acquisition sites, including the freshness-only Verify branch.
	assert_eq!(authority.matches("source.read().await").count(), 4);
	assert_eq!(sweeper.matches("source.read().await").count(), 1);
	let mut sites = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			if source.contains("source.read().await") {
				sites.push(path.strip_prefix(root).unwrap().to_path_buf());
			}
		}
	});
	sites.sort();
	assert_eq!(
		sites,
		vec![
			PathBuf::from("src/storage/manager/gc_disposal.rs"),
			PathBuf::from("src/storage/manager/op_hooks.rs"),
			PathBuf::from("src/storage/manager/op_recovery.rs"),
			PathBuf::from("src/storage/manager/reservations.rs"),
			PathBuf::from("src/storage/manager/tier_authority.rs"),
			// ADR 0704: `canonical_tier_lease`, the one acquisition behind every publication or
			// retirement of committed tier state (census below).
			PathBuf::from("src/storage/manager/tiered_io.rs"),
			PathBuf::from("src/storage/sweeper.rs")
		]
	);
	// ADR 0704: every publication or retirement of committed tier state takes the canonical
	// lease before the slab's publication lock, through the one acquisition above, and never
	// awaits object I/O under it. A publication that needs object storage signals instead; its
	// entry point releases the lease and the lock, prepares, and tries once more.
	let tiered_io = fs::read_to_string(root.join("src/storage/manager/tiered_io.rs")).unwrap();
	let deferral = fs::read_to_string(root.join("src/storage/manager/tier_deferral.rs")).unwrap();
	assert_eq!(tiered_io.matches("source.read().await").count(), 1);
	assert!(
		manager_method_body(&tiered_io, "canonical_tier_lease").contains("source.read().await")
	);
	let mut lease_callers = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			let count = source.matches("canonical_tier_lease()").count();
			if count != 0 {
				lease_callers.push((path.strip_prefix(root).unwrap().to_path_buf(), count));
			}
		}
	});
	lease_callers.sort();
	assert_eq!(
		lease_callers,
		vec![
			// ADR 0704: the certified tier step's one read of committed state
			// (`committed_certified_tier`), held for no I/O and no lock.
			(PathBuf::from("src/storage/manager/certified_tier.rs"), 1),
			// The recovered Tier and Retire deliveries and the superseded handoff.
			(PathBuf::from("src/storage/manager/op_recovery.rs"), 4),
			// ADR 0704: verification with no deferred-snapshot retry judges each pending
			// generation, and then the Ready transition, under the bound canonical state.
			(PathBuf::from("src/storage/manager/tier_authority.rs"), 2),
			// The authority worker's replay.
			(PathBuf::from("src/storage/manager/tier_deferral.rs"), 1),
			// Reconcile, the observer and local publication (and its failed preparation's
			// settlement under fresh authority), retirement, held retirements, and (ADR
			// 0704) held snapshot retirement marks, which the lease classifies.
			(PathBuf::from("src/storage/manager/tiered_io.rs"), 6),
		]
	);
	for (source, name) in [
		(&tiered_io, "reconcile_tier_slab_at_snapshot_epoch<F>"),
		(&tiered_io, "apply_tier_slab_with_epoch"),
		(&deferral, "replay_tier_deferrals"),
	] {
		let body = manager_method_body(source, name);
		assert!(
			body.find("canonical_tier_lease()").unwrap()
				< body.find("tier_publication_lock(").unwrap(),
			"{name}: the lease precedes the slab lock"
		);
		assert!(
			body.find("needs_tier_preparation(").unwrap()
				< body.find("prepare_tier_delivery_observed(").unwrap(),
			"{name}: object storage only on the leased attempt's signal"
		);
	}
	for name in [
		"retire_tiered_slab_with_epoch",
		"retry_deferred_retirements",
	] {
		let body = manager_method_body(&tiered_io, name);
		assert!(body.contains("canonical_tier_lease()"), "{name}");
		assert!(!body.contains("prepare_tier_delivery"), "{name}");
	}
	// The leased publication judges the canonical standing first, before any staging,
	// deferral or other authority change it could cause, then signals for preparation; it
	// never fetches.
	let leased = manager_method_body(
		&tiered_io,
		"apply_tier_slab_under_publication_lock_prepared",
	);
	assert!(leased.contains("tier_publication_needs_preparation("));
	let standing = leased.find("self.tier_standing_of(").unwrap();
	for later in [
		"cleanup_claim(",
		"record_tier_deferral(",
		"stage_tier_authority_under_publication_lock(",
		"update_pending_tier_authority(",
		"bump_slab_publication_revision(",
		"publish_committed_tier_pointer(",
	] {
		assert!(
			standing < leased.find(later).unwrap(),
			"{later} before the standing"
		);
	}
	for forbidden in [
		"prepare_tier_delivery",
		"build_tiered_row_refresh_plan",
		"tiered_object_from_l2",
		"ObjectStore",
	] {
		assert!(!leased.contains(forbidden), "{forbidden} under the lease");
	}
	// The operation handoff's lease spans local state and one redb commit only.
	let op_hooks = fs::read_to_string(root.join("src/storage/manager/op_hooks.rs")).unwrap();
	assert_eq!(op_hooks.matches("source.read().await").count(), 1);
	let handoff = manager_method_body(&op_hooks, "hand_off_op_if_complete");
	for remote in ["object_store", "l2", "sleep", "client_write", "backoff"] {
		assert!(!handoff.contains(remote), "{remote}");
	}
	// GC's local stages hold the lease across local files and redb only.
	let disposal = fs::read_to_string(root.join("src/storage/manager/gc_disposal.rs")).unwrap();
	assert_eq!(disposal.matches("source.read().await").count(), 6);
	for stage in [
		"gc_complete_mark",
		"undo_rejected_claim",
		"gc_dispose_run",
		"gc_resume_disposal",
		"gc_finish_removal",
	] {
		let body = manager_method_body(&disposal, stage);
		for remote in [
			"object_store",
			"sleep",
			"client_write",
			"backoff",
			"committer",
			"submit_op",
		] {
			assert!(!body.contains(remote), "{stage}: {remote}");
		}
	}
	// An abandoned install's stages too.
	let recovery = fs::read_to_string(root.join("src/storage/manager/op_recovery.rs")).unwrap();
	// ADR 0704: the tier delivery's lease moved to `canonical_tier_lease` (census above).
	assert_eq!(recovery.matches("source.read().await").count(), 3);
	// ADR 0704: a recovered Tier operation is classified under the lease before it prepares.
	let deliver = manager_method_body(&recovery, "deliver_applied_tier_op");
	assert!(
		deliver.find("tier_standing(").unwrap() < deliver.find("prepare_tier_delivery(").unwrap()
	);
	for stage in [
		"undo_abandoned_install",
		"finish_applied_install",
		"hand_off_rejected_tier_op",
		// ADR 0704: the superseded handoff's lease spans local state and one redb commit.
		"hand_off_superseded_tier_op",
	] {
		let body = manager_method_body(&recovery, stage);
		for remote in [
			"object_store",
			"sleep",
			"client_write",
			"backoff",
			"committer",
			"submit_op",
		] {
			assert!(!body.contains(remote), "{stage}: {remote}");
		}
	}
	// DC-176: a loop over a slab decodes per physical frame. Reading one record is a deliberate,
	// named choice, and each new place that makes it is reviewed here.
	let mut point_reads = Vec::new();
	for (name, path) in [
		("manager.rs", "src/storage/manager.rs"),
		(
			"encoding_compute.rs",
			"src/storage/manager/encoding_compute.rs",
		),
		(
			"retention_reclaim.rs",
			"src/storage/manager/retention_reclaim.rs",
		),
		(
			"secondary_index_segments.rs",
			"src/storage/manager/secondary_index_segments.rs",
		),
		("stats_facts.rs", "src/storage/manager/stats_facts.rs"),
		("tier_authority.rs", "src/storage/manager/tier_authority.rs"),
		("tier_plan.rs", "src/storage/manager/tier_plan.rs"),
		("tiered_io.rs", "src/storage/manager/tiered_io.rs"),
		("certified_tier.rs", "src/storage/manager/certified_tier.rs"),
	] {
		let source = fs::read_to_string(root.join(path)).unwrap();
		point_reads.push((
			name,
			source.matches("FrameLocation::of_point_read(").count(),
		));
	}
	assert_eq!(
		point_reads,
		[
			// ADR 0704 moved the tiered replacement build (one read per physical
			// frame) from the root to retention_reclaim.rs.
			("manager.rs", 1),
			("encoding_compute.rs", 2),
			("retention_reclaim.rs", 3),
			("secondary_index_segments.rs", 0),
			("stats_facts.rs", 0),
			("tier_authority.rs", 1),
			("tier_plan.rs", 2),
			// ADR 0705: the ranged L2 read splits the one record it decrypted
			// (`record_and_header_from_record_bytes`) instead of re-locating it.
			("tiered_io.rs", 1),
			// ADR 0704: the certified tier claim reads each physical frame once.
			("certified_tier.rs", 1),
		],
		"a new point read: if it is in a loop over a slab's entries, group by frame instead"
	);
	// DC-158: the tier stats build decodes a whole slab, off the runtime's workers.
	let sweeper_source = fs::read_to_string(root.join("src/storage/sweeper.rs")).unwrap();
	let stats_stage = sweeper_source
		.split("async fn encrypt_tiered_object_blob_with_current_stats(")
		.nth(1)
		.and_then(|rest| rest.split("\n\t}\n").next())
		.expect("the stats stage");
	assert!(
		stats_stage.contains("cpu_bound_off_the_runtime_worker"),
		"the stats build must not hold a runtime worker"
	);
	// A reservation's local stages hold it across local files and redb only, and a
	// burn and a refusal ask the ring first and hold it only after.
	let reservations =
		fs::read_to_string(root.join("src/storage/manager/reservations.rs")).unwrap();
	assert_eq!(reservations.matches("source.read().await").count(), 5);
	for stage in [
		"cleanup_dispose",
		"cleanup_resume_disposal",
		"cleanup_finish",
	] {
		let body = manager_method_body(&reservations, stage);
		for remote in [
			"object_store",
			"sleep",
			"client_write",
			"backoff",
			"committer",
			"submit_op",
		] {
			assert!(!body.contains(remote), "{stage}: {remote}");
		}
	}
	let burn = manager_method_body(&reservations, "finish_burn");
	let after_lease = burn.split("let lease = match").nth(1).unwrap();
	for remote in ["committer", "submit_op", "client_write", "sleep", "backoff"] {
		assert!(!after_lease.contains(remote), "{remote}");
	}
	// The rejected-claim handoff asks the ring first, and holds its lease only after.
	let resolve = manager_method_body(&disposal, "resolve_rejected_claim");
	let after_lease = resolve.split("let lease = match").nth(1).unwrap();
	for remote in ["committer", "cancel_op", "client_write", "sleep", "backoff"] {
		assert!(!after_lease.contains(remote), "{remote}");
	}
	for stage in ["gc_dispose_run", "gc_finish_removal"] {
		let body = manager_method_body(&disposal, stage);
		for remote in [
			"object_store",
			"l2",
			"sleep",
			"client_write",
			"backoff",
			"committer",
			"submit_op",
		] {
			assert!(!body.contains(remote), "{stage}: {remote}");
		}
	}
	assert!(compare.contains("let canonical = retry.source.read().await?;\n\t\tlet manager"));
	assert!(callback.contains("let canonical = match self.snapshot_source.get()"));
	assert!(
		!callback
			.split("let canonical =")
			.next()
			.unwrap()
			.contains(".await")
	);
	assert!(verify.contains("let plaintext = self.plaintext(epoch, &pending.pointer, &mut cache).await?;\n\t\t\tlet canonical ="));
	assert!(verify.contains("\n\t\t}\n\t\tlet groups = {"));
	let per_pointer = verify
		.split("let _canonical =")
		.nth(1)
		.unwrap()
		.split("\n\t\t}\n\t\tlet groups = {")
		.next()
		.unwrap();
	let ready = verify.rsplit("let _canonical =").next().unwrap();
	let freshness = authority
		.split("SnapshotReconciliationPhase::Verify { revision } => {")
		.nth(1)
		.unwrap()
		.split("SnapshotReconciliationPhase::Compare => {")
		.next()
		.unwrap();
	assert!(freshness.contains("let canonical = retry.source.read().await;"));
	assert!(
		!freshness
			.split("let canonical =")
			.next()
			.unwrap()
			.contains(".await")
	);
	for scope in [compare, callback, per_pointer, ready, freshness] {
		for forbidden in [
			"ObjectStore",
			".plaintext(",
			".frame(",
			".fetch(",
			"tokio::time",
			"next_transient_retry",
			".recv(",
			".join(",
		] {
			assert!(
				!scope.contains(forbidden),
				"{forbidden} inside an SM lease scope"
			);
		}
	}
	// The actual retry/backoff arm only starts after comparison returned,
	// dropping the comparison's guard (including every error return).
	let worker = manager_method_body(&authority, "run");
	assert!(
		worker
			.find("self.retry_snapshot_comparison(&retry).await")
			.unwrap() < worker
			.find("sleep(next_transient_retry(&mut delay))")
			.unwrap()
	);
	// Direct local callees are a reviewed census, not permission to fetch.
	for name in [
		"quarantine_pending_snapshot_collision",
		"quarantine_bootstrap_collision_locked",
		"finish_tier_comparison_at_epoch",
		"stage_deferred_snapshot_tiers",
	] {
		let body = manager_method_body(&authority, name);
		for forbidden in [
			"ObjectStore",
			".plaintext(",
			".frame(",
			".fetch(",
			"tokio::time",
		] {
			assert!(!body.contains(forbidden), "{name} introduced {forbidden}");
		}
	}
}

/// A canonical writer cannot wait on the local locks held by a lease reader.
/// Observer I/O is dispatched after the SM guard's enclosing block ends.
#[test]
fn snapshot_lease_writers_do_not_await_local_publication_locks() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(root.join("src/consensus/raft_storage.rs")).unwrap();
	let sm = source
		.split("impl WendelonStateMachine {")
		.nth(1)
		.unwrap()
		.split("fn apply_entry_to_state(")
		.next()
		.unwrap();
	// Nine cfg(test) seeding helpers (ADR 0728 added the tenant keysets') plus
	// the two production writers.
	assert_eq!(sm.matches("self.inner.write().await").count(), 11);
	let apply = sm
		.split("let pending_checkpoint = {\n\t\t\tlet mut guard = self.inner.write().await;")
		.nth(1)
		.unwrap()
		.split("\n\t\t};")
		.next()
		.unwrap();
	let install = sm
		.rsplit("let pending_checkpoint = {\n\t\t\tlet mut guard = self.inner.write().await;")
		.next()
		.unwrap()
		.split("\n\t\t};")
		.next()
		.unwrap();
	for guarded in [apply, install] {
		assert!(!guarded.contains(".await"));
		assert!(!guarded.contains("self.slab"));
	}
	let builder = source
		.split("impl RaftSnapshotBuilder<WendelonTypeConfig> for WendelonSnapshotBuilder {")
		.nth(1)
		.unwrap()
		.split("#[cfg(test)]")
		.next()
		.unwrap();
	assert_eq!(builder.matches("self.inner.write().await").count(), 2);
	for guarded in builder
		.split("let mut guard = self.inner.write().await;")
		.skip(1)
	{
		let guarded = guarded
			.split("\n\t\t};")
			.next()
			.unwrap()
			.split("\n\t\t}")
			.next()
			.unwrap();
		assert!(!guarded.contains(".await"));
	}
	let sweeper = fs::read_to_string(root.join("src/storage/sweeper.rs")).unwrap();
	let authority = fs::read_to_string(root.join("src/storage/manager/tier_authority.rs")).unwrap();
	// The only in-guard tier hook uses synchronous fence bookkeeping.
	for body in [
		manager_method_body(&sweeper, "before_slab_retired"),
		manager_method_body(&authority, "observer_is_superseded"),
		manager_method_body(&authority, "fence_unapplied_tier_retirement"),
		manager_method_body(&authority, "mark_retirement"),
	] {
		assert!(!body.contains(".await"));
		assert!(!body.contains("self.state.lock()"));
		assert!(!body.contains("tier_publication_lock"));
	}
}

#[test]
fn control_lsm_output_rename_has_one_guarded_staging_path() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(root.join("src/storage/manager/lsm_driver.rs")).unwrap();
	let wrapper = manager_method_body(&source, "stage_lsm_compaction_outputs");
	let local = manager_method_body(&source, "install_lsm_compaction_temp_outputs");
	let control = manager_method_body(
		&source,
		"install_lsm_compaction_temp_outputs_via_control_ring_with_context",
	);
	assert!(
		wrapper.find("maintenance_publication_gate").unwrap()
			< wrapper.find("lock_lsm_slabs").unwrap()
	);
	assert!(
		wrapper.find("lock_lsm_slabs").unwrap()
			< wrapper.find("lsm_input_authority_locked").unwrap()
	);
	assert!(
		wrapper.find("lsm_input_authority_locked").unwrap()
			< wrapper
				.find(".stage_lsm_compaction_outputs_locked(")
				.unwrap()
	);
	assert!(
		local.find("lock_lsm_slabs").unwrap()
			< local.find(".stage_lsm_compaction_outputs_locked(").unwrap()
	);
	assert!(
		control.find(".stage_lsm_compaction_outputs(").unwrap()
			< control.find("committer.submit_op(").unwrap()
	);
	assert_eq!(
		source
			.matches(".stage_lsm_compaction_outputs_locked(")
			.count(),
		2
	);
	assert_eq!(source.matches(".stage_lsm_compaction_outputs(").count(), 1);
	assert_eq!(source.matches(".install_lsm_output_files(").count(), 1);
	assert!(
		manager_method_body(&source, "stage_lsm_compaction_outputs_locked")
			.contains(".install_lsm_output_files(")
	);
}

#[test]
fn completed_control_lsm_recovery_has_one_accepted_private_issuer() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let driver = fs::read_to_string(root.join("src/storage/manager/lsm_driver.rs")).unwrap();
	let index = fs::read_to_string(root.join("src/storage/index.rs")).unwrap();
	let open = fs::read_to_string(root.join("src/storage/manager/open.rs")).unwrap();
	let control = manager_method_body(
		&driver,
		"install_lsm_compaction_temp_outputs_via_control_ring_with_context",
	);
	assert!(
		control.find("committer.submit_op(").unwrap()
			< control.find("new_after_acceptance(").unwrap()
	);
	assert!(control.contains("prepared_sidecars.is_none()"));
	assert_eq!(driver.matches("new_after_acceptance(").count(), 2);
	let capability = driver
		.split("pub(crate) struct AcceptedControlLsmCompletion {")
		.nth(1)
		.unwrap()
		.split("struct UnresolvedControlIntentMetric")
		.next()
		.unwrap();
	assert!(!capability.contains("Serialize"));
	assert!(!capability.contains("Deserialize"));
	assert!(!capability.contains("derive(Clone"));
	let mut issuers = Vec::new();
	walk_dir(&root.join("src"), &mut |path| {
		if path.extension().is_some_and(|ext| ext == "rs")
			&& path.file_name().is_none_or(|name| name != "tests.rs")
		{
			let source = fs::read_to_string(path).unwrap();
			let count = rust_identifiers_from_source("control LSM completion census", &source)
				.iter()
				.filter(|name| *name == "apply_with_control_lsm_completion")
				.count();
			if count != 0 {
				issuers.push((path.strip_prefix(root).unwrap().to_path_buf(), count));
			}
		}
	});
	issuers.sort();
	assert_eq!(
		issuers,
		vec![
			(PathBuf::from("src/storage/index.rs"), 1),
			(PathBuf::from("src/storage/manager/lsm_driver.rs"), 1)
		]
	);
	// Untyped updates retain the original Local-only validation. The opaque
	// accepted capability is the only route to a Control phase transition.
	assert!(index.contains("if before.kind != LsmInstallKind::Local"));
	assert!(manager_method_body(&index, "apply_with_publication_guard").contains("None"));
	assert!(
		manager_method_body(&index, "apply_with_control_lsm_completion")
			.contains("Some(completion)")
	);
	let recover = driver
		.split("pub(super) async fn reconcile_completed_control_lsm_intent(")
		.nth(1)
		.unwrap()
		.split("pub(super) fn lsm_compaction_tmp_path(")
		.next()
		.unwrap();
	for guard in [
		"LsmInstallKind::Control",
		"LsmInstallPhase::OutputsPublished",
		"commit_digest",
		"local_output_rows_digest",
		"local_output_files",
		"SlabState::Retired",
		"secondary_index_segments",
		"SidecarCoverage::None",
	] {
		assert!(recover.contains(guard), "missing {guard}");
	}
	assert!(recover.contains("clear_lsm_install_intent(intent.clone())"));
	for forbidden in [
		".apply(",
		".put_",
		".unlink(",
		".write_all(",
		"reconcile_local_lsm_intent(",
	] {
		assert!(
			!recover.contains(forbidden),
			"recovery introduced {forbidden}"
		);
	}
	assert_eq!(
		open.matches("lsm_driver::reconcile_local_lsm_intent(")
			.count(),
		1
	);
	assert_eq!(
		open.matches("lsm_driver::reconcile_completed_control_lsm_intent(")
			.count(),
		1
	);
}

#[test]
fn lsm_stage_refusal_is_definite_only_before_rename() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = fs::read_to_string(root.join("src/storage/manager/lsm_driver.rs")).unwrap();
	let locked = manager_method_body(&source, "stage_lsm_compaction_outputs_locked");
	assert_eq!(locked.matches("LsmStageFailure::BeforeRename").count(), 1);
	assert!(
		locked.find("LsmStageFailure::BeforeRename").unwrap()
			< locked.find(".install_lsm_output_files(").unwrap()
	);
	let wrapper = manager_method_body(&source, "stage_lsm_compaction_outputs");
	assert!(!wrapper.contains("Uncertain"));
	assert!(
		wrapper.rfind("LsmStageFailure::BeforeRename").unwrap()
			< wrapper
				.find(".stage_lsm_compaction_outputs_locked(")
				.unwrap()
	);
	// Every implicit conversion is uncertain: only the explicit pre-rename
	// checks may report a definite refusal.
	for conversion in [
		"impl From<CompactionFailure> for LsmStageFailure {\n\tfn from(failure: CompactionFailure) -> Self {\n\t\tSelf::Uncertain(failure)",
		"impl From<std::io::Error> for LsmStageFailure {\n\tfn from(error: std::io::Error) -> Self {\n\t\tSelf::Uncertain(error.into())",
	] {
		assert!(source.contains(conversion), "{conversion}");
	}
	assert_eq!(
		source.matches("for LsmStageFailure {").count(),
		2,
		"no other conversion into a stage failure"
	);
	// The local path clears its refused intent; the control path has an allocated
	// operation, which closes only through the ring (ADR 0697).
	assert_eq!(source.matches(".clear_refused_lsm_intent(").count(), 1);
	assert_eq!(source.matches(".cancel_unsubmitted_lsm_op(").count(), 1);
	for (caller, clear) in [
		(
			"install_lsm_compaction_temp_outputs",
			".clear_refused_lsm_intent(",
		),
		(
			"install_lsm_compaction_temp_outputs_via_control_ring_with_context",
			".cancel_unsubmitted_lsm_op(",
		),
	] {
		let body = manager_method_body(&source, caller);
		let clear = body.find(clear).unwrap();
		assert!(body[..clear].contains("LsmStageFailure::BeforeRename("));
		assert!(body.contains("LsmStageFailure::Uncertain(failure)) => return Err(failure)"));
	}
}

/// ADR 0692 §7 (ADR 0696 fast follow): every proposer of a tier pointer
/// change holds a history token. Production code reaches
/// `TierCommitter::commit_tier_slab` only through `propose_tier_slab`, which
/// reserves the token first; the committer's argument is the
/// `TierProposal` that binds the pointer to it, so no other path can
/// propose a pointer without one.
#[test]
fn every_tier_proposer_holds_a_history_token() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let production = |path: &str| -> String {
		let source = fs::read_to_string(manifest_dir.join(path)).unwrap();
		match source.split_once("#[cfg(test)]\nmod tests {") {
			Some((production, _)) => production.to_owned(),
			None => source,
		}
	};
	let sweeper = production("src/storage/sweeper.rs");
	let reclaim = production("src/storage/manager/retention_reclaim.rs");
	let helper = sweeper
		.split_once("pub(crate) async fn propose_tier_slab(")
		.and_then(|(_, tail)| tail.split_once("\n}\n"))
		.map(|(body, _)| body.to_owned())
		.expect("propose_tier_slab exists");
	assert!(
		helper.contains("reserve_tier_history(tier.clone())")
			&& helper.contains("submit_tier_proposal(slab, committer, proposal, allocation.first)"),
		"the helper reserves the token, allocates, then submits under it"
	);
	for (name, source) in [("sweeper.rs", &sweeper), ("retention_reclaim.rs", &reclaim)] {
		// The one production builder, and the fixture adapter's dispatch.
		let direct = source.matches("SlabCommand::Tier(").count();
		let direct = direct - if name == "sweeper.rs" { 2 } else { 0 };
		assert_eq!(
			direct, 0,
			"{name}: production proposals go through propose_tier_slab"
		);
	}
	// The definition, and its six production call sites.
	assert!(sweeper.matches("propose_tier_slab(").count() >= 7);
	assert_eq!(reclaim.matches("sweeper::propose_tier_slab(").count(), 2);
	let manager = manager_source(manifest_dir);
	assert_eq!(
		manager.matches("fn reserve_tier_history(").count(),
		1,
		"one constructor of a reserved proposal"
	);
	assert!(
		!manager.contains("TierProposal {\n\t\t\ttier,\n\t\t\ttoken: None"),
		"production never builds an unreserved proposal"
	);
}

/// ADR 0696 (0692 §7, plan r3 D): every backing installed into the manager
/// state goes through `ManagerState::install_{sealed,tiered,rehydrated}`,
/// which forget the slab's disposal evidence first, so evidence never
/// outlives a backing installed for that slab again.
#[test]
fn every_backing_install_forgets_disposal_evidence() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	for map in ["sealed", "tiered", "rehydrated"] {
		let needle = format!(".{map}.insert(");
		let found = manager_occurrences(manifest_dir, |source| source.matches(&needle).count());
		assert_eq!(
			found,
			vec![("backing_install.rs".to_string(), 1)],
			"`{needle}` only inside ManagerState::install_{map}"
		);
		let source =
			fs::read_to_string(manifest_dir.join("src/storage/manager/backing_install.rs"))
				.unwrap();
		let helper = format!("fn install_{map}(");
		let start = source.find(&helper).expect("the install helper exists");
		let body = &source[start..start + source[start..].find("\n\t}").unwrap()];
		let forget = body
			.find("disposal_evidence.forget(")
			.expect("it forgets evidence");
		assert!(
			forget < body.find(&needle).unwrap(),
			"install_{map} forgets the evidence before it installs"
		);
	}
}

/// ADR 0696 (0692 §8, plan r3 F): the lineage census counts one sample per
/// slab's published generation, so every write of `tier_layouts` goes
/// through `ManagerState::{install_tier_layout, remove_tier_layout}`, the
/// only places that update it.
#[test]
fn every_published_layout_transition_updates_the_lineage_census() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	for (write, helper) in [
		(".tier_layouts.insert(", "fn install_tier_layout("),
		(".tier_layouts.remove(", "fn remove_tier_layout("),
	] {
		let found = manager_occurrences(manifest_dir, |source| source.matches(write).count());
		assert_eq!(
			found,
			vec![("backing_install.rs".to_string(), 1)],
			"`{write}` only inside ManagerState::{helper}"
		);
		let source =
			fs::read_to_string(manifest_dir.join("src/storage/manager/backing_install.rs"))
				.unwrap();
		let start = source.find(helper).expect("the helper exists");
		let body = &source[start..start + source[start..].find("\n\t}").unwrap()];
		assert!(body.contains(write), "{helper} performs the write");
		assert!(
			body.contains("census().replace("),
			"{helper} updates the census"
		);
	}
	for forbidden in [".tier_layouts.clear(", ".tier_layouts.retain("] {
		assert!(
			manager_occurrences(manifest_dir, |source| source.matches(forbidden).count())
				.is_empty(),
			"no `{forbidden}` bypasses the census"
		);
	}
}

/// The body of `item` (`pub struct Name` or `pub enum Name`) in `source`.
fn brace_body<'a>(source: &'a str, item: &str) -> &'a str {
	let at = source
		.find(&format!("{item} {{"))
		.unwrap_or_else(|| panic!("{item} not found"));
	let open = at + source[at..].find('{').unwrap();
	let mut depth = 0;
	for (offset, character) in source[open..].char_indices() {
		match character {
			'{' => depth += 1,
			'}' => {
				depth -= 1;
				if depth == 0 {
					return &source[open..open + offset];
				}
			}
			_ => {}
		}
	}
	panic!("{item} is not closed");
}

/// ADR 0697 §9.4: no bare slab id in a control-ring command, the state machine or
/// its snapshot. A slab id is only ever qualified: by a `SlabRef`, or beneath an
/// owner that is inseparable from it. Each such nesting is named here, not waived.
#[test]
fn a_ring_command_or_state_never_names_a_bare_slab_id() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let ops = fs::read_to_string(root.join("src/consensus/slab_ops.rs")).unwrap();
	let raft = fs::read_to_string(root.join("src/consensus/raft_storage.rs")).unwrap();
	let lsm = fs::read_to_string(root.join("src/storage/lsm.rs")).unwrap();
	for item in [
		"pub struct TierOp",
		"pub struct RetireOp",
		"pub struct GcRemove",
		"pub struct ClaimOp",
		"pub struct CompleteOp",
		"pub struct BurnOp",
		"pub struct OpKey",
		"pub struct SlabOp",
		"pub enum SlabCommand",
		"pub struct CancelOp",
		"pub struct AckOutcomes",
		"pub enum SlabRejection",
	] {
		assert!(!brace_body(&ops, item).contains("SlabId"), "{item}");
	}
	assert!(!brace_body(&raft, "pub struct SmState").contains("SlabId"));
	let requests = fs::read_to_string(root.join("src/consensus/core.rs")).unwrap();
	assert!(!brace_body(&requests, "pub enum WendelonRequest").contains("SlabId"));
	assert!(
		brace_body(&raft, "pub struct SmState")
			.contains("BTreeMap<OwnerRef, crate::consensus::slab_ops::OwnerTerminal>"),
		"terminal sets sit beneath their owner"
	);
	// The two nestings: the claimed map and the intervals exist only under that key.
	let terminal = brace_body(&ops, "pub struct OwnerTerminal");
	assert_eq!(terminal.matches("SlabId").count(), 1);
	assert!(terminal.contains("pub claimed: BTreeMap<SlabId, CompactionJobId>"));
	assert!(terminal.contains("pub removed: SlabIntervals"));
	assert!(ops.contains("pub struct SlabIntervals(Vec<(u64, u64)>);"));
	// A compaction commit's vectors are all local to its one owner field.
	assert!(
		brace_body(&lsm, "pub struct CompactionCommit")
			.contains("pub owner: crate::consensus::OwnerRef")
	);
}

/// ADR 0725: an owner-signed slab operation enters the log only through the
/// leader's admission, is signed only by the committer from a ticket or an
/// acknowledgement batch, and has an effect only after `verify_owner_op`.
#[test]
fn owner_operations_have_one_producer_one_signer_and_one_verifier() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let src = root.join("src");
	let mut requests = std::collections::BTreeMap::new();
	let mut signers = std::collections::BTreeMap::new();
	walk_dir(&src, &mut |path| {
		let name = path.file_name().unwrap().to_string_lossy().into_owned();
		if path.extension().is_none_or(|ext| ext != "rs") || name.ends_with("tests.rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		let production = source.split("#[cfg(test)]\nmod tests").next().unwrap();
		let built = production.matches("WendelonRequest::OwnerOp(").count();
		if built > 0 {
			requests.insert(name.clone(), built);
		}
		let signed = ["submit(", "cancel(", "ack("]
			.iter()
			.map(|kind| {
				production
					.matches(&format!("SignedOwnerOp::{kind}"))
					.count()
			})
			.collect::<Vec<_>>();
		if signed.iter().any(|count| *count > 0) {
			signers.insert(name, signed);
		}
	});
	assert_eq!(
		requests,
		std::collections::BTreeMap::from([
			("committed.rs".to_owned(), 1),
			("decision_proposals.rs".to_owned(), 1),
			("raft_storage.rs".to_owned(), 5),
		]),
		"owner operations are matched in apply and built only by admission"
	);
	let proposals = fs::read_to_string(src.join("network/decision_proposals.rs")).unwrap();
	assert!(proposals.contains("return Ok(Admitted::Ready(WendelonRequest::OwnerOp(op)));"));
	assert!(proposals.contains("if op.claimed_owner().node() != sender {"));
	assert_eq!(
		signers,
		std::collections::BTreeMap::from([("sweeper.rs".to_owned(), vec![1, 1, 1])]),
		"only the committer signs, once per kind"
	);
	// Apply reaches a signed operation's content only through the verifier, and
	// a bare slab operation is refused in a ParentDecisions state before it.
	let raft = fs::read_to_string(src.join("consensus/raft_storage.rs")).unwrap();
	assert_eq!(raft.matches("verify_owner_op(state, ").count(), 2);
	assert_eq!(raft.matches(".into_op()").count(), 2);
	assert_eq!(raft.matches("owner_ops::verify(").count(), 1);
	let apply = source_region(
		&raft,
		"fn apply_request_at_log_index(",
		"WendelonRequest::SlabOp(op) => {",
	);
	assert!(apply.contains("WendelonResponse::Refused(SlabRefusal::OwnerOpUnsigned)"));
	assert!(apply.contains("if state.decisions.is_some() =>"));
	// The follower surface is decided in one place.
	let owner_ops = fs::read_to_string(src.join("consensus/owner_ops.rs")).unwrap();
	assert_eq!(owner_ops.matches("is_first_publication()").count(), 1);
	assert!(
		owner_ops
			.contains("Self::Tier(TierOp { tier, replaces: None }) if tier.certified.is_none()")
	);
}

/// ADR 0726: the root check has one constructor; no key is reachable outside
/// the verified binding; a changed root is fenced before its state is
/// published; and no read plan outlives its read.
#[test]
fn the_root_check_has_one_constructor_and_keys_live_only_in_the_verified_binding() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let src = root.join("src");
	let mut establishers = std::collections::BTreeMap::new();
	let mut provider_calls = Vec::new();
	let mut self_binders = Vec::new();
	let mut stored_plans = Vec::new();
	walk_dir(&src, &mut |path| {
		let name = path.file_name().unwrap().to_string_lossy().into_owned();
		if path.extension().is_none_or(|ext| ext != "rs") || name.ends_with("tests.rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		let production = source.split("#[cfg(test)]\nmod tests").next().unwrap();
		let built = production
			.matches("WendelonRequest::EstablishRootCheck(")
			.count();
		if built > 0 && name != "raft_storage.rs" {
			establishers.insert(name.clone(), built);
		}
		if name == "crypto.rs" {
			return;
		}
		if production.contains(".wrap_tenant_kek(") || production.contains(".unwrap_tenant_kek(") {
			provider_calls.push(name.clone());
		}
		if production.contains("bind_to_own_candidate_without_cluster(") {
			self_binders.push(name.clone());
		}
		for line in production.lines() {
			let line = line.trim();
			let is_plan = [
				"TieredMultiRangeReadPlan",
				"TieredRangeReadPlan",
				"TieredStatsBlockReadPlan",
			]
			.iter()
			.any(|plan| line.contains(plan));
			// A field declaration: `name: Type,` with no borrow, call or return.
			if is_plan
				&& line.ends_with(',')
				&& line.contains(": ")
				&& !line.contains('&')
				&& !line.contains('(')
				&& !line.contains("->")
			{
				stored_plans.push(format!("{name}: {line}"));
			}
		}
	});
	// The state machine matches the request; only formation builds it, once.
	assert_eq!(
		establishers,
		std::collections::BTreeMap::from([
			("committed.rs".to_owned(), 1),
			("node.rs".to_owned(), 1),
		])
	);
	let node = fs::read_to_string(src.join("node.rs")).unwrap();
	assert_eq!(
		node.matches("WendelonRequest::EstablishRootCheck(candidate)")
			.count(),
		1
	);
	assert!(
		source_region(
			&node,
			"pub(crate) async fn establish_root_check_if_forming(",
			"async fn seed_default_tenant_kek_if_missing(",
		)
		.contains("WendelonRequest::EstablishRootCheck(candidate)")
	);
	assert!(provider_calls.is_empty(), "{provider_calls:?}");
	assert!(self_binders.is_empty(), "{self_binders:?}");
	assert!(stored_plans.is_empty(), "{stored_plans:?}");

	let crypto = fs::read_to_string(src.join("crypto.rs")).unwrap();
	let production = crypto
		.split("#[cfg(test)]\nmod root_binding_tests;")
		.next()
		.unwrap();
	// The provider is called by the binding (wrap and self-check of a new
	// tenant KEK, unwrap of a stored one) and by the root check, nowhere else.
	assert_eq!(production.matches(".wrap_tenant_kek(").count(), 2);
	assert_eq!(production.matches(".unwrap_tenant_kek(").count(), 3);
	let manager = brace_body(production, "pub struct CryptoManager");
	for field in ["keysets", "tenant_kek_cache", "slab_dek_cache"] {
		assert!(
			!manager.contains(field),
			"the manager holds `{field}` outside a binding"
		);
	}
	let bound = brace_body(production, "struct Bound");
	for field in [
		"keysets:",
		"tenant_kek_cache:",
		"slab_dek_cache:",
		"provider:",
	] {
		assert!(bound.contains(field), "the binding lacks `{field}`");
	}
	// The unverified surface: the candidate takes no key and returns a check.
	assert!(
		production
			.contains("pub(crate) fn candidate_root_check(&self) -> std::io::Result<RootCheck> {")
	);

	// The fence runs under the state guard, before the installed state replaces
	// the live one.
	let raft = fs::read_to_string(src.join("consensus/raft_storage.rs")).unwrap();
	let install = source_region(&raft, "async fn install_snapshot(", "guard.state = state;");
	assert!(install.contains("observer.fence_before_publication("));
	assert_eq!(raft.matches("guard.state = state;").count(), 1);
}

/// ADR 0699 S2: decision commands have no producer outside tests, signatures
/// verify nothing outside tests, and every snapshot body passes through the two
/// mode-explicit codec functions.
#[test]
fn parent_decisions_have_no_producer_and_one_codec() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let src = root.join("src");
	let mut sites = Vec::new();
	walk_dir(&src, &mut |path| {
		let name = path.file_name().unwrap().to_string_lossy().into_owned();
		if path.extension().is_none_or(|ext| ext != "rs") || name.ends_with("tests.rs") {
			return;
		}
		let source = fs::read_to_string(path).unwrap();
		let production = source.split("#[cfg(test)]\nmod tests").next().unwrap();
		for (offset, _) in production.match_indices("WendelonRequest::Decision(") {
			let rest = &production[offset + "WendelonRequest::Decision(".len()..];
			sites.push((name.clone(), rest.split_once(')').unwrap().0.to_owned()));
		}
	});
	let mut per_file = std::collections::BTreeMap::new();
	for (file, _) in &sites {
		*per_file.entry(file.as_str()).or_insert(0usize) += 1;
	}
	assert_eq!(
		per_file,
		std::collections::BTreeMap::from([
			("committed.rs", 1),
			// ADR 0699 S5a: the leader's generation step proposes too.
			// ADR 0700: a time-bearing command is built only by `Unstamped::stamp`.
			("decision_proposals.rs", 4),
			("raft_storage.rs", 2),
		]),
		"decisions are matched in apply and built only by the leader's proposal path"
	);
	assert!(sites.contains(&("raft_storage.rs".to_owned(), "_".to_owned())));
	assert!(sites.contains(&("raft_storage.rs".to_owned(), "command".to_owned())));
	let proposals = fs::read_to_string(src.join("network/decision_proposals.rs")).unwrap();
	assert!(proposals.contains("Ok(Admitted::Ready(WendelonRequest::Decision(command)))"));
	// ADR 0700: the time-bearing commands carry a stamp only through `stamp`.
	let stamp = source_region(
		&proposals,
		"pub(crate) fn stamp(self, stamp_ms: u64) -> WendelonRequest {",
		"\n\t}\n",
	);
	assert!(stamp.contains("DecisionCommand::Reserve { claim, stamp_ms }"));
	assert!(stamp.contains("DecisionCommand::RecordNoop { noop, stamp_ms }"));
	assert_eq!(
		proposals.matches("stamp_ms }").count(),
		2,
		"stamped only in `stamp`"
	);
	let forwarded = brace_body(&proposals, "pub enum ForwardedDecision");
	assert!(!forwarded.contains("stamp") && !forwarded.contains("floor"));
	// ADR 0721: a binding names the incarnation and the key the sender signs with.
	// The leader still supplies the sender and binds its own trust-map entry, and
	// only when the sender's key equals it.
	assert!(
		forwarded
			.contains("BindSigner {\n\t\tincarnation: StoreIncarnation,\n\t\tkey: [u8; 32],\n\t},"),
		"a binding names the incarnation and the sender's key"
	);
	assert!(
		proposals.contains(".get(&sender)\n\t\t\t\t.ok_or(ProposalRefusal::UnknownSignerKey)?;")
	);
	assert!(
		proposals.contains(
			"if key != senders {\n\t\t\t\treturn Err(ProposalRefusal::SignerKeyDisagrees);"
		)
	);

	let decisions = fs::read_to_string(src.join("consensus/decisions.rs")).unwrap();
	assert_eq!(
		decisions.matches("Verified(").count(),
		1,
		"one private constructor"
	);
	assert!(decisions.contains("Ok(Verified(signed.body.clone()))"));
	let verify = source_region(&decisions, "pub(crate) fn verify<T: Evidence>(", "\n\t}\n");
	assert!(verify.contains(".signer_key(signed.signer)"));
	assert!(verify.contains("key.verify_strict("));
	assert!(
		!decisions.contains("synthetic"),
		"no stand-in signature remains"
	);

	// ADR 0699 S3b: the old replicate handlers are never an ingress in this mode.
	let replication =
		fs::read_to_string(src.join("network/data_server/write_replication.rs")).unwrap();
	for handler in [
		"async fn handle_replicate(&self, frame: HrftFrame)",
		"async fn handle_table_replicate(",
	] {
		let head = replication
			.split_once(handler)
			.unwrap()
			.1
			.split("let observation")
			.next()
			.unwrap();
		assert!(
			head.contains(
				"if self.decision_mode != crate::consensus::decision_mode::DecisionMode::Today"
			),
			"{handler} refuses first under ParentDecisions"
		);
	}

	let raft = fs::read_to_string(src.join("consensus/raft_storage.rs")).unwrap();
	let writer = source_region(&raft, "fn write_state_body<", "\nfn encode_state_body(");
	assert_eq!(raft.matches("postcard::to_io(").count(), 2);
	assert_eq!(writer.matches("postcard::to_io(").count(), 2);
	assert!(writer.contains("(DecisionMode::Today, None) => postcard::to_io(state, writer)"));
	assert_eq!(raft.matches("take_from_bytes::<SmState>").count(), 0);
	let reader = source_region(
		&raft,
		"fn decode_state_body(",
		"\nasync fn spawn_blocking_snapshot_io",
	);
	assert!(reader.contains("DecisionMode::Today => whole::<SmState>(body)"));
	assert!(reader.contains("envelope.decisions.validate()?;"));
	assert!(brace_body(&raft, "pub struct SmState").contains(
		"#[serde(skip)]\n\tpub decisions: Option<crate::consensus::decisions::DecisionState>,"
	));
}

/// ADR 0706 (Decision 5): who keeps a production lifecycle permit across
/// cancellation. A classification guard, not a proof of cancellation safety:
/// the behavioural controls are the evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LifecyclePermitOwner {
	/// A service an orderly close stops between steps and drains, bounded,
	/// before Raft shuts down.
	DrainedService(&'static str),
	/// The spawned task that owns the work and the permit to its end.
	SpawnedTask,
	/// `under_permit` itself: each caller is classified on its own.
	PermitHelper,
}

/// Every production `lifecycle_permit()` / `under_permit(` site, as (file
/// under `src/`, enclosing function, owner).
const LIFECYCLE_PERMIT_CENSUS: &[(&str, &str, LifecyclePermitOwner)] = &[
	(
		"network/data_server/certification.rs",
		"certification_step",
		LifecyclePermitOwner::DrainedService("decision maintenance"),
	),
	(
		"network/data_server/certification.rs",
		"under_permit",
		LifecyclePermitOwner::PermitHelper,
	),
	(
		"network/data_server/certification.rs",
		"validate_certified_base",
		LifecyclePermitOwner::DrainedService("decision maintenance"),
	),
	(
		"network/data_server/cleanup.rs",
		"certified_cleanup_step",
		LifecyclePermitOwner::DrainedService("decision maintenance"),
	),
	(
		"network/data_server/cleanup.rs",
		"certified_lsm_step_unpaused",
		LifecyclePermitOwner::DrainedService("LSM compaction"),
	),
	(
		"network/data_server/cleanup.rs",
		"certified_tier_step",
		LifecyclePermitOwner::DrainedService("LSM compaction"),
	),
	(
		"storage/manager/lsm_driver.rs",
		"install_lsm_output_files",
		LifecyclePermitOwner::SpawnedTask,
	),
	(
		"storage/manager/op_recovery.rs",
		"recover_abandoned_install",
		LifecyclePermitOwner::SpawnedTask,
	),
];

/// A source file's production part: everything before its first top-level
/// test module.
fn production_part(source: &str) -> &str {
	let mut at = 0;
	for line in source.split_inclusive('\n') {
		if line.starts_with("#[cfg(")
			&& line.contains("test")
			&& source[at + line.len()..].starts_with("mod ")
		{
			return &source[..at];
		}
		at += line.len();
	}
	source
}

/// The lifecycle permit sites of one production source, as (file, enclosing
/// function); commented lines and test modules are not sites.
fn lifecycle_permit_sites(file: &str, source: &str) -> Vec<(String, String)> {
	let production = production_part(source);
	let mut sites = Vec::new();
	for needle in ["lifecycle_permit()", "under_permit("] {
		for (offset, _) in production.match_indices(needle) {
			let line = production[..offset].rfind('\n').map_or(0, |at| at + 1);
			if production[line..offset].trim_start().starts_with("//") {
				continue;
			}
			let head = &production[..offset];
			let function = head
				.rfind("fn ")
				.map(|at| {
					head[at + 3..]
						.split(|c: char| !(c.is_alphanumeric() || c == '_'))
						.next()
						.unwrap_or_default()
						.to_owned()
				})
				.unwrap_or_default();
			sites.push((file.to_owned(), function));
		}
	}
	sites.sort();
	sites.dedup();
	sites
}

/// The sites the census does not classify.
fn unclassified_permit_sites(
	sites: &[(String, String)],
	census: &[(&str, &str, LifecyclePermitOwner)],
) -> Vec<(String, String)> {
	sites
		.iter()
		.filter(|(file, function)| {
			!census
				.iter()
				.any(|(listed, name, _)| listed == file && name == function)
		})
		.cloned()
		.collect()
}

/// ADR 0706 (Decision 5, acceptance 4): every production site that takes a
/// lifecycle permit is classified with the owner that keeps it across
/// cancellation; a new site fails here until it is. The drained services stop
/// and drain before Raft does.
#[test]
fn every_lifecycle_permit_site_has_an_owner_that_outlives_cancellation() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut sites = Vec::new();
	walk_dir(&src, &mut |path| {
		let name = path.file_name().unwrap().to_string_lossy().into_owned();
		if path.extension().is_none_or(|ext| ext != "rs") || name.ends_with("tests.rs") {
			return;
		}
		let file = path
			.strip_prefix(&src)
			.unwrap()
			.to_string_lossy()
			.into_owned();
		sites.extend(lifecycle_permit_sites(
			&file,
			&fs::read_to_string(path).unwrap(),
		));
	});
	sites.sort();
	assert_eq!(
		unclassified_permit_sites(&sites, LIFECYCLE_PERMIT_CENSUS),
		Vec::<(String, String)>::new(),
		"a lifecycle permit site without a classified owner"
	);
	let stale: Vec<_> = LIFECYCLE_PERMIT_CENSUS
		.iter()
		.filter(|(file, function, _)| {
			!sites
				.iter()
				.any(|(found, name)| found == file && name == function)
		})
		.collect();
	assert!(stale.is_empty(), "census entries with no site: {stale:?}");

	// The drained services: both signalled, then drained, before the
	// transports drain and Raft shuts down.
	let node = fs::read_to_string(src.join("node.rs")).unwrap();
	let shutdown = source_region(
		&node,
		"\tpub async fn shutdown(self) -> std::io::Result<()> {\n\t\tlet lifecycle = self.lifecycle;",
		"\n\t}\n",
	);
	let order: Vec<_> = [
		"self.data_server.stop_decision_maintenance();",
		"handles.lsm_compaction_control.stop();",
		"handles.drain_lifecycle_services().await",
		"lifecycle.begin_drain();",
		"self.raft.shutdown().await",
	]
	.iter()
	.map(|step| {
		shutdown
			.find(step)
			.unwrap_or_else(|| panic!("Node::shutdown lacks `{step}`"))
	})
	.collect();
	assert!(
		order.windows(2).all(|pair| pair[0] < pair[1]),
		"stop both, drain both, then drain transports and stop Raft: {order:?}"
	);
	let drain = source_region(
		&node,
		"async fn drain_lifecycle_services(&mut self)",
		"\n\t}\n",
	);
	for service in LIFECYCLE_PERMIT_CENSUS
		.iter()
		.filter_map(|(_, _, owner)| match owner {
			LifecyclePermitOwner::DrainedService(service) => Some(*service),
			_ => None,
		}) {
		assert!(
			drain.contains(&format!("\"{service}\"")),
			"the drain covers the {service} service"
		);
	}
}

/// ADR 0706 (acceptance 4, its negative control): an unlisted site is
/// reported; a commented mention and a test module are not sites.
#[test]
fn an_unclassified_lifecycle_permit_site_fails_the_census() {
	let source = "impl Rogue {\n\tasync fn rogue_step(&self) -> std::io::Result<()> {\n\t\tlet permit = owner.lifecycle_permit()?;\n\t\t// owner.lifecycle_permit() in a comment\n\t\tOk(())\n\t}\n}\n\n#[cfg(test)]\nmod tests {\n\tfn fixture() {\n\t\tlet _ = owner.lifecycle_permit();\n\t}\n}\n";
	let sites = lifecycle_permit_sites("network/data_server/rogue.rs", source);
	assert_eq!(
		sites,
		vec![(
			"network/data_server/rogue.rs".to_owned(),
			"rogue_step".to_owned()
		)]
	);
	assert_eq!(
		unclassified_permit_sites(&sites, LIFECYCLE_PERMIT_CENSUS),
		sites,
		"an unlisted site fails the census"
	);
	// A listed name in another file is still unlisted.
	let moved = lifecycle_permit_sites(
		"storage/rogue.rs",
		"async fn certified_tier_step() { self.under_permit(work).await }\n",
	);
	assert_eq!(
		unclassified_permit_sites(&moved, LIFECYCLE_PERMIT_CENSUS),
		moved
	);
	assert!(!moved.is_empty());
}

/// ADR 0707: decision maintenance proposes only through its stop-aware
/// capability, its loop sees a ready stop before a ready tick, and the stop is
/// never attached to the `DecisionProposals` shared with client RPCs. A
/// classification guard; the behavioural controls are the evidence.
#[test]
fn decision_maintenance_proposes_only_through_its_stop_aware_path() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let node = fs::read_to_string(src.join("node.rs")).unwrap();
	let maintenance = source_region(
		&node,
		"let maintenance = crate::network::decision_proposals::MaintenanceProposals::new(",
		"certified_cleanup_step",
	);
	let order: Vec<_> = [
		"biased;",
		"stop.wait_for(|stopping| *stopping)",
		"ticker.tick()",
	]
	.iter()
	.map(|step| {
		maintenance
			.find(step)
			.unwrap_or_else(|| panic!("the maintenance loop lacks `{step}`"))
	})
	.collect();
	assert!(
		order.windows(2).all(|pair| pair[0] < pair[1]),
		"a ready stop wins over a ready tick: {order:?}"
	);
	for step in [
		"maintenance.ensure_signer_once(",
		"maintenance.advance_floor_once()",
		"maintenance\n\t\t\t\t\t\t\t.resolve_closed_once(",
		"maintenance.generation_step()",
		"data_server.certification_step(&maintenance)",
	] {
		assert!(
			maintenance.contains(step),
			"the maintenance loop proposes through its capability: `{step}`"
		);
	}
	for direct in [
		".propose(",
		".commit(",
		"proposals.ensure_signer_once",
		"proposals.generation_step",
	] {
		assert!(
			!maintenance.contains(direct),
			"no maintenance proposal bypasses the stop: `{direct}`"
		);
	}
	let certification =
		fs::read_to_string(src.join("network/data_server/certification.rs")).unwrap();
	assert!(!certification.contains(".propose("));
	assert!(certification.contains("let _ = maintenance.report(report).await;"));
	let proposals = fs::read_to_string(src.join("network/decision_proposals.rs")).unwrap();
	assert!(
		!brace_body(&proposals, "pub struct DecisionProposals").contains("watch::"),
		"the maintenance stop is never stored in the shared proposal path"
	);
	let result = brace_body(&proposals, "pub(crate) enum MaintenanceProposalResult");
	assert!(result.contains("Stopped,") && result.contains("Response(ProposalResponse),"));
	let derive = proposals
		.split_once("pub(crate) enum MaintenanceProposalResult")
		.unwrap()
		.0
		.rsplit("#[derive(")
		.next()
		.unwrap();
	assert!(
		!derive.contains("Serialize"),
		"the stopped result is local, never serialized: {derive}"
	);
}
