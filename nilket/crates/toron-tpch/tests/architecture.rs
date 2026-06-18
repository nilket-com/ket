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
