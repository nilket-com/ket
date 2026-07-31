//! ADR 261 architecture guards for the TPC-H reachability harness.

use std::path::Path;

fn source(relative: &str) -> String {
	let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
	std::fs::read_to_string(&path)
		.unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

#[test]
fn adr261_tpch_helper_is_not_a_toron_library_dependency() {
	let toron_manifest = source("../toron/Cargo.toml");
	assert!(
		!toron_manifest.contains("toron-tpch"),
		"the toron library must not depend on the reachability harness"
	);
}

#[test]
fn adr448_filtered_path_projects_exactly_once() {
	let lib = source("src/lib.rs");
	assert!(
		!lib.contains("append_projection_to_pbc"),
		"projection must not be injected into PBC on only one filter arm"
	);
	let production = lib
		.split("#[cfg(test)]")
		.next()
		.expect("production harness source");
	assert_eq!(
		production.matches("filtered.project(").count(),
		2,
		"ledger and streaming paths must each have one post-filter projection"
	);
}

#[test]
fn adr448_paired_driver_probes_capability_and_records_arms() {
	let main = source("src/main.rs");
	assert!(main.contains("\"adr448-capability\""));
	assert!(main.contains("ADR448_HARNESS_CAPABILITY"));
	assert!(main.contains("TORON_ABLATE_LOCAL_RUNTIME_FILTER"));
	assert!(main.contains("ablation_arm"));
	assert!(main.contains("paired_ablation_arm"));
}

#[test]
fn adr261_no_official_tpc_tooling_in_tree() {
	for relative in [
		"Cargo.toml",
		"src/lib.rs",
		"src/main.rs",
		"src/telemetry.rs",
	] {
		let lowered = source(relative).to_ascii_lowercase();
		for forbidden in ["dbgen", "qgen", "reqwest", "ureq", "download"] {
			assert!(
				!lowered.contains(forbidden),
				"{relative} must not reference official TPC tooling / auto-fetch: {forbidden}"
			);
		}
	}
}

#[test]
fn adr261_no_second_arrow_recordbatch_boundary() {
	// First-cut dev data is built from `tpchgen` rows into workspace-Arrow; the Arrow-output generator
	// variant pins a different Arrow major and must not be a dependency.
	assert!(
		!source("Cargo.toml").contains("tpchgen-arrow"),
		"the helper must not depend on the Arrow-output generator variant in the first cut"
	);
}

#[test]
fn adr261_blocked_requires_recognized_reason() {
	let telemetry = source("src/telemetry.rs");
	assert!(
		telemetry.contains("RECOGNIZED_BLOCKERS"),
		"the recognized-blocker allowlist exists"
	);
	assert!(
		telemetry.contains("fn status_for_reason") && telemetry.contains("Status::Fail"),
		"an unrecognized reason routes to Fail, not Blocked"
	);
}

#[test]
fn adr261_stable_phase_status_telemetry() {
	let telemetry = source("src/telemetry.rs");
	for phase in [
		"\"generate\"",
		"\"load\"",
		"\"plan\"",
		"\"bind\"",
		"\"execute\"",
		"\"compare\"",
		"\"fragment\"",
	] {
		assert!(telemetry.contains(phase), "phase string {phase} is pinned");
	}
	for status in ["\"pass\"", "\"blocked\"", "\"fail\"", "\"not_attempted\""] {
		assert!(
			telemetry.contains(status),
			"status string {status} is pinned"
		);
	}
}

#[test]
fn adr261_cache_root_is_target_not_tracked() {
	assert!(
		source("src/lib.rs").contains("\"target/tpch\""),
		"the cache/output root defaults under target/, never a tracked path"
	);
}

#[test]
fn adr262_strict_q6_attempts_decimal128_compute() {
	let lib = source("src/lib.rs");
	assert!(
		lib.contains("Decimal128")
			&& lib.contains("fn generate_strict_lineitem")
			&& lib.contains("STRICT_Q6_SQL"),
		"strict mode builds a Decimal128(15,2) fixture + strict Q6"
	);
	assert!(
		lib.contains("fn run_strict_q6") && lib.contains("run_query("),
		"strict run goes through the shared plan/bind/execute runner"
	);
	let probe = source("tests/decimal_probe.rs");
	assert!(
		probe.contains("decimal128_compute_binds_and_executes")
			&& probe.contains("decimal_ddl_blocks_at_plan"),
		"both probes are retained as tests (ADR 263 moved the compute probe past the bind wall)"
	);
}

#[test]
fn adr262_first_red_row_is_grounded_not_predeclared() {
	let lib = source("src/lib.rs");
	// stage errors are classified from the actual engine message and retain that same message for diagnostics,
	// not a hardcoded/predeclared reason.
	assert!(
		lib.contains("fn from_message(phase: Phase, message: String) -> Self")
			&& lib.contains("let reason = reason_for_error(&message).to_string()")
			&& lib.contains("message,"),
		"the run path classifies the engine error message"
	);
	// the strict run delegates to the shared, classified runner (no predeclared reason in its body)
	assert!(
		lib.contains("fn run_strict_q6") && lib.contains("run_query("),
		"strict run does not predeclare a blocker"
	);
}

#[test]
fn adr262_matrix_regenerated_from_jsonl() {
	let lib = source("src/lib.rs");
	assert!(
		lib.contains("reachability.jsonl") && lib.contains("fn matrix_from_log"),
		"the matrix regenerates from the JSONL log; no hand-maintained matrix state"
	);
}

#[test]
fn adr262_status_is_inventory_not_last_result() {
	let lib = source("src/lib.rs");
	let tests = source("tests/harness.rs");
	assert!(
		lib.contains("fn status_rows") && !lib.contains("strict=blocked"),
		"status is static inventory and never encodes a measured `blocked` result"
	);
	assert!(
		tests.contains("status_marks_q06_strict_implemented_not_last_result"),
		"the status-is-inventory behavior is pinned"
	);
}

#[test]
fn adr293_q07_is_wired_as_a_single_physical_nation_self_join() {
	let lib = source("src/lib.rs");
	let main = source("src/main.rs");
	let harness = source("tests/harness.rs");
	assert!(lib.contains("pub const Q7_DEV_SQL"));
	assert!(lib.contains("STRICT_Q7_SQL"));
	assert!(lib.contains("nation n1, nation n2"));
	assert!(lib.contains("fn q07_tables"));
	assert!(lib.contains("table.name != \"region\""));
	assert!(
		!lib.contains("nation_1") && !lib.contains("nation_2"),
		"q07 must use SQL aliases over one physical nation table"
	);
	assert!(lib.contains("|| *id == \"q07\""));
	assert!(main.contains("\"q07\" | \"q7\""));
	assert!(harness.contains("q07_executes_in_both_modes"));
}

#[test]
fn adr297_q08_reuses_q05_tables_plus_one_physical_nation() {
	let lib = source("src/lib.rs");
	let main = source("src/main.rs");
	let harness = source("tests/harness.rs");
	assert!(lib.contains("pub const Q8_DEV_SQL"));
	assert!(lib.contains("STRICT_Q8_SQL"));
	assert!(lib.contains("nation n1, nation n2"));
	assert!(lib.contains("fn q08_tables"));
	assert!(lib.contains("q05_tables(strict, scale_factor)"));
	assert!(lib.contains("name: \"part\""));
	assert!(
		!lib.contains("nation_1") && !lib.contains("nation_2"),
		"q08 must use SQL aliases over one physical nation table"
	);
	assert!(lib.contains("|| *id == \"q08\""));
	assert!(main.contains("\"q08\" | \"q8\""));
	assert!(harness.contains("q08_executes_in_both_modes"));
}

#[test]
fn adr430_tpch_execution_paths_share_disk_backed_join_spill_placement() {
	let lib = source("src/lib.rs");
	assert_eq!(
		lib.matches("ExecutionContext::local_test()").count(),
		1,
		"all TPC-H contexts, including explicit-budget census contexts, must use one constructor"
	);
	assert!(
		lib.contains("fn tpch_execution_context_with_budget(")
			&& lib.contains("fn tpch_execution_context(")
			&& lib.contains("tpch_execution_context_with_budget(task_telemetry, budget)")
			&& lib.contains(".with_join_spill_store(")
			&& lib.contains("PathBuf::from(CACHE_ROOT).join(\"join_spill\")"),
		"the centralized default/explicit-budget contexts must share untracked join-spill placement"
	);
}

#[test]
fn adr449_gate_zero_is_profile_first_and_oracle_derived() {
	let main = source("src/main.rs");
	assert!(main.contains("adr449-gate-zero"));
	assert!(main.contains("TORON_GROUPED_CHAIN_PROFILE_DIR"));
	assert!(main.contains("profiled_reservoir_build_rows"));
	assert!(main.contains("q05_reservoir_full_input_cardinality"));
	assert!(main.contains("on_rows < off_rows"));
	assert!(main.contains("off_rows == expected_off"));
	assert!(main.contains("run_adr449_gate_zero"));
}

#[test]
fn adr449_pairing_is_parity_checked_and_gate_zero_admissible() {
	let main = source("src/main.rs");
	assert!(main.contains("paired_ablation_differences"));
	assert!(main.contains("differences_off_minus_on_ms") && main.contains("off.ok_or"));
	assert!(main.contains("paired receipt requires a passed gate-zero record"));
	assert!(main.contains("ablation arm parity mismatch"));
	assert!(main.contains("paired_ablation"));
}

#[test]
fn adr450_scale_ladder_classifies_only_known_non_commits() {
	let main = source("src/main.rs");
	assert!(main.contains("adr450-scale-ladder"));
	assert!(main.contains("ADR450_GUARD_A"));
	assert!(main.contains("ADR450_GUARD_B"));
	assert!(main.contains("ADR450_GUARD_A") && main.contains("ADR450_GUARD_B"));
	assert!(main.contains("scale rung hard failure"));
	assert!(main.contains("adr450_selected_scale.json"));
	assert!(main.contains("selected_scale_or_default"));
}

#[test]
fn adr450_ablation_arms_share_the_same_parquet_projection_union() {
	let lib = source("src/lib.rs");
	let projection = lib
		.find("let Some(columns) = &pbc_projection")
		.expect("shared projection union");
	let with_projection = lib[projection..]
		.find("builder = builder.with_projection(mask)")
		.map(|offset| projection + offset)
		.expect("shared Parquet projection");
	let inflater = lib[with_projection..]
		.find("inflate_projected_parquet_batch(")
		.map(|offset| with_projection + offset)
		.expect("projected-batch inflater");
	assert!(
		with_projection < inflater,
		"both arms must use the same projected union before inflation"
	);
	assert!(lib.matches("inflate_projected_parquet_batch(").count() >= 3);
}

#[test]
fn adr528_fragment_census_posture_and_phase_do_not_leak() {
	// lib.rs interleaves inline #[cfg(test)] items with production code, so this guard scans
	// the whole file; the assignment patterns are receiver-specific (`config.`), which keeps
	// test-local mutations out of the counts.
	let lib = source("src/lib.rs");
	assert_eq!(
		lib.matches("config.single_partition_execution = false;")
			.count(),
		1,
		"the distributed posture is set in exactly one place: the census bind config"
	);
	assert_eq!(
		lib.matches("config.single_partition_execution = true;")
			.count(),
		1,
		"default gauge rails keep the single-partition posture in exactly one place"
	);
	for vocabulary in [
		"\"bind_rejected\"",
		"\"fragment_rejected\"",
		"\"fragments_sequential\"",
		"\"fragments_parallel\"",
	] {
		assert!(
			lib.contains(vocabulary),
			"census classification {vocabulary} is pinned"
		);
	}
	assert!(
		!source("src/main.rs").contains("Phase::Fragment"),
		"the CLI never constructs the fragment phase; census rows come from the library rail"
	);
	assert!(
		source("src/telemetry.rs").contains("\"tpch_fragment_census\""),
		"the census suite name is pinned"
	);
}

#[test]
fn adr529_execution_census_is_explicit_and_does_not_leak_workers() {
	let lib = source("src/lib.rs");
	let main = source("src/main.rs");
	let telemetry = source("src/telemetry.rs");
	assert!(main.contains("\"execution-census\" => execution_census_command"));
	assert!(telemetry.contains("\"tpch_execution_census\""));
	assert_eq!(
		lib.matches("DistributedQueryCoordinator::new(").count(),
		1,
		"the explicit ADR 529 rail is the harness library's sole coordinator constructor"
	);
	assert_eq!(
		lib.matches("ToronWorker::with_partition_store_and_endpoint(")
			.count(),
		1,
		"the explicit ADR 529 rail is the harness library's sole worker constructor"
	);
	assert!(
		lib.contains("InMemoryPartitionStore::with_budget(budget)"),
		"the census must retain the production worker's in-memory partition store semantics"
	);
	assert!(
		!main.contains("Phase::Fragment"),
		"the CLI cannot counterfeit a typed preflight phase"
	);
}
