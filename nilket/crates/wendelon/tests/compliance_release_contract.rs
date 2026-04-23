use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_repo_file(relative: &str) -> String {
	fs::read_to_string(repo_root().join(relative)).unwrap_or_else(|err| {
		panic!("{relative} should be readable: {err}");
	})
}

fn assert_contains(haystack: &str, needle: &str, context: &str) {
	assert!(
		haystack.contains(needle),
		"{context} should contain `{needle}`"
	);
}

#[test]
fn fips_profile_is_explicit_and_symbol_sweep_uses_it() {
	let cargo = read_repo_file("Cargo.toml");
	assert_contains(&cargo, "[profile.fips]", "Cargo.toml");
	assert_contains(&cargo, "inherits = \"release\"", "Cargo.toml");
	let fips_section = cargo
		.split("[profile.fips]")
		.nth(1)
		.expect("Cargo.toml should define profile.fips")
		.lines()
		.take_while(|line| !line.trim_start().starts_with('['))
		.collect::<Vec<_>>()
		.join("\n");
	for inherited_setting in [
		"opt-level",
		"lto",
		"codegen-units",
		"panic",
		"strip",
		"debug",
	] {
		assert!(
			!fips_section.contains(inherited_setting),
			"profile.fips should inherit `{inherited_setting}` from profile.release instead of restating it"
		);
	}

	let sweep = read_repo_file("ci/check_fips_symbol_sweep.sh");
	for required in [
		"cargo build --locked --profile fips --no-default-features --features fips --bin wendelon",
		"target_dir=\"${CARGO_TARGET_DIR:-target}\"",
		"binary=\"${target_dir}/fips/wendelon\"",
		"AES-GCM-SIV",
		"blake3-256",
		"CARGO_PROFILE_FIPS_STRIP",
	] {
		assert_contains(&sweep, required, "FIPS symbol sweep script");
	}
}

#[test]
fn fips_startup_self_tests_are_code_backed_and_documented() {
	let build_info = read_repo_file("src/build_info.rs");
	for required in [
		"aws_lc_rs::try_fips_mode()",
		"run_fips_startup_known_answer_tests",
		"SHA256_ABC_KAT",
		"AES256_GCM_EMPTY_PLAINTEXT_KAT",
		"FIPS startup self-test failed",
	] {
		assert_contains(&build_info, required, "src/build_info.rs");
	}

	let evidence = read_repo_file("plans/compliance/self_test_evidence.md");
	for required in [
		"aws_lc_rs::try_fips_mode()",
		"SHA-256",
		"AES-256-GCM",
		"FIPS startup self-test failed",
		"cargo test --lib build_info::tests",
		"CC=clang CXX=clang++ cargo test --lib --no-default-features --features fips build_info::tests",
	] {
		assert_contains(&evidence, required, "compliance self-test evidence");
	}
}

#[test]
fn compliance_artifacts_exist_and_name_current_boundary_gaps() {
	for path in [
		"plans/compliance/crypto_boundary_inventory.md",
		"plans/compliance/fips_build_profile.md",
		"plans/compliance/self_test_evidence.md",
		"plans/compliance/release_artifact_evidence.md",
		"plans/compliance/cmvp_status_evidence.md",
		"plans/compliance/compliance_pack_index.md",
		"plans/compliance/external/cmvp_lab_contact_record.md",
	] {
		assert!(
			repo_root().join(path).exists(),
			"compliance artifact should exist: {path}"
		);
	}

	let boundary = read_repo_file("plans/compliance/crypto_boundary_inventory.md");
	for required in [
		"ed25519-dalek",
		"scrypt",
		"rustls",
		"ring",
		"Current FIPS-profile gap",
		"not FIPS-certified",
	] {
		assert_contains(&boundary, required, "compliance boundary inventory");
	}
}

#[test]
fn release_evidence_scripts_cover_sbom_signing_and_attestation_contract() {
	let generate = read_repo_file("ci/generate_regulated_release_evidence.sh");
	for required in [
		"cargo metadata --locked",
		"git status --porcelain --untracked-files=all",
		"cargo sbom --output-format spdx_json_2_3",
		"sbom.spdx.json.missing",
		"artifact_sha256",
		"build_time_utc",
		"attestation_path",
		"attestation_missing",
		"release-manifest.sha256",
		"openssl dgst -sha256",
		"provenance.json.sig",
		"WENDELON_RELEASE_SIGNING_KEY",
		"WENDELON_ATTESTATION_STATEMENT",
		"attestation.intoto.jsonl",
	] {
		assert_contains(&generate, required, "regulated evidence generator");
	}

	let verify = read_repo_file("ci/verify_regulated_release_evidence.sh");
	for required in [
		"REQUIRE_SPDX_SBOM",
		"REQUIRE_RELEASE_SIGNING",
		"REQUIRE_ATTESTATION",
		"WENDELON_RELEASE_VERIFY_KEY",
		"[[ -e \"${out_dir}/${signature_file}\" ]]",
		"signature exists but WENDELON_RELEASE_VERIFY_KEY is not set",
		"manifest_lists",
		"release-manifest.sha256 is missing required entry",
		"release-manifest.sha256 is missing present SBOM",
		"release-manifest.sha256 is missing present attestation",
		"provenance.json must not contain both attestation_path and attestation_missing",
		"provenance.json names attestation_path but attestation.intoto.jsonl is missing",
		"provenance.json names attestation_missing but attestation.intoto.jsonl is present",
		"provenance.json must contain attestation_path or attestation_missing",
		"provenance.json.sig",
		"release-manifest.sha256.sig",
		"provenance.json does not embed the binary digest",
		"sha256sum -c",
	] {
		assert_contains(&verify, required, "regulated evidence verifier");
	}
	assert!(
		!generate.contains("\"attestation\": \"attestation.intoto.jsonl\""),
		"provenance should not imply attestation output exists when only the .missing sidecar is present"
	);
}

#[test]
fn cmvp_status_language_remains_narrow() {
	let cmvp = read_repo_file("plans/compliance/cmvp_status_evidence.md");
	for required in [
		"not FIPS-certified",
		"not CMVP validated",
		"not represented in this repo as being in validation",
		"not a CMVP certificate",
		"plans/compliance/external/cmvp_lab_contact_record.md",
	] {
		assert_contains(&cmvp, required, "CMVP status evidence");
	}

	let lab_record = read_repo_file("plans/compliance/external/cmvp_lab_contact_record.md");
	for required in [
		"no external lab contact recorded",
		"not evidence of lab engagement",
		"must not claim",
	] {
		assert_contains(&lab_record, required, "CMVP lab contact record");
	}
}
