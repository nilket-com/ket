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
	// stage errors are classified from the actual engine message, not hardcoded
	assert!(
		lib.contains("reason_for_error(&format!"),
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
