use std::collections::BTreeSet;
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

fn table_rows(content: &str) -> Vec<Vec<String>> {
	content
		.lines()
		.map(str::trim)
		.filter(|line| line.starts_with('|') && line.ends_with('|'))
		.filter(|line| !line.contains("|---"))
		.map(|line| {
			line.trim_matches('|')
				.split('|')
				.map(|cell| cell.trim().to_string())
				.collect::<Vec<_>>()
		})
		.collect()
}

fn code_value(cell: &str) -> &str {
	cell.trim().trim_matches('`')
}

fn backtick_tokens(cell: &str) -> Vec<&str> {
	cell.split('`')
		.enumerate()
		.filter_map(|(index, token)| (index % 2 == 1).then_some(token.trim()))
		.collect()
}

fn repo_path_tokens(cell: &str) -> Vec<&str> {
	backtick_tokens(cell)
		.into_iter()
		.filter(|token| {
			token.contains('/')
				&& !token.contains(',')
				&& !token.contains(' ')
				&& !token.starts_with("http")
		})
		.collect()
}

fn assert_repo_path_exists(relative: &str, context: &str) {
	let path = repo_root().join(relative);
	assert!(
		path.exists(),
		"{context} references missing repo path `{relative}`"
	);
}

fn adr_ids_on_disk() -> BTreeSet<String> {
	let adr_dir = repo_root().join("plans/adr");
	fs::read_dir(adr_dir)
		.expect("plans/adr should be readable")
		.flatten()
		.filter_map(|entry| {
			let name = entry.file_name();
			let name = name.to_string_lossy();
			let id = name.get(0..3)?;
			id.chars()
				.all(|ch| ch.is_ascii_digit())
				.then(|| id.to_string())
		})
		.collect()
}

#[test]
fn adr_status_matrix_covers_every_adr_with_owner_and_gate() {
	let content = read_repo_file("plans/impl/adr_status_matrix.md");
	let mut seen = BTreeSet::new();

	for row in table_rows(&content) {
		if row.len() < 8 || row[0] == "ADR" {
			continue;
		}

		let adr_id = code_value(&row[0]);
		if adr_id.len() != 3 || !adr_id.chars().all(|ch| ch.is_ascii_digit()) {
			continue;
		}

		seen.insert(adr_id.to_string());

		let source_paths = repo_path_tokens(&row[1]);
		assert_eq!(
			source_paths.len(),
			1,
			"ADR {adr_id} should have exactly one source path"
		);
		assert_repo_path_exists(source_paths[0], &format!("ADR {adr_id} status row"));

		assert!(!code_value(&row[3]).is_empty(), "ADR {adr_id} needs status");
		assert!(!row[4].is_empty(), "ADR {adr_id} needs an owner");
		assert!(
			row[5].contains("`M"),
			"ADR {adr_id} needs a blocking milestone"
		);
		assert!(
			row[6].starts_with("2026-"),
			"ADR {adr_id} needs a concrete last review date"
		);
		assert!(!row[7].is_empty(), "ADR {adr_id} needs a next action");
	}

	assert_eq!(seen, adr_ids_on_disk());
}

#[test]
fn design_lock_register_has_required_control_fields() {
	let content = read_repo_file("plans/gap/design_lock_20260422.md");
	for required in [
		"## Maintained Register",
		"Decision",
		"Owner",
		"Date locked",
		"Unblock target",
		"Downstream files/workstreams affected",
		"Reopen condition",
	] {
		assert!(content.contains(required), "missing `{required}`");
	}

	let mut ids = BTreeSet::new();
	for row in table_rows(&content) {
		if row.len() < 9 || !row[0].starts_with("`DL-") {
			continue;
		}
		let id = code_value(&row[0]).to_string();
		assert!(!row[1].is_empty(), "{id} needs a decision");
		assert!(!row[2].is_empty(), "{id} needs an owner");
		assert!(row[3].starts_with("2026-"), "{id} needs a concrete date");
		assert!(row[4].contains("`M"), "{id} needs an unblock target");
		assert!(!row[6].is_empty(), "{id} needs downstream impact");
		assert_eq!(code_value(&row[7]), "locked", "{id} should be locked");
		assert!(!row[8].is_empty(), "{id} needs a reopen condition");
		ids.insert(id);
	}

	for expected in 1..=12 {
		assert!(
			ids.contains(&format!("DL-{expected:03}")),
			"missing DL-{expected:03}"
		);
	}
}

#[test]
fn artifact_registry_present_paths_exist() {
	let content = read_repo_file("plans/impl/artifact_registry.md");
	for row in table_rows(&content) {
		if row.len() < 5 || row[0] == "Artifact" {
			continue;
		}

		let state = code_value(&row[4]);
		assert!(
			matches!(state, "present" | "reserved"),
			"artifact `{}` has unknown state `{state}`",
			row[0]
		);

		if state == "present" {
			let paths = repo_path_tokens(&row[2]);
			assert!(
				!paths.is_empty(),
				"present artifact `{}` needs a canonical repo path",
				row[0]
			);
			for path in paths {
				assert_repo_path_exists(path, &format!("artifact `{}`", row[0]));
			}
		}
	}
}

#[test]
fn scorecard_rows_point_at_known_sources() {
	let content = read_repo_file("plans/impl/scorecard.md");
	for row in table_rows(&content) {
		if row.len() < 5 || row[0] == "Gate" {
			continue;
		}

		for path in repo_path_tokens(&row[4]) {
			assert_repo_path_exists(path, &format!("scorecard row `{}`", row[1]));
		}
	}
}

#[test]
fn scorecard_includes_every_milestone_gate() {
	let content = read_repo_file("plans/impl/scorecard.md");
	for milestone in [
		"M0 Foundation",
		"M1 Correctness Locked",
		"M2 Scale & Survive",
		"M3 Customer Pilot Ready",
		"M4 Regulated Pilot Authorization",
		"M5 Regulated GA",
	] {
		assert!(
			content.contains(&format!("`{milestone}`")),
			"scorecard should include `{milestone}`"
		);
	}
}

#[test]
fn external_dependency_tracker_names_required_calendar_dependencies() {
	let content = read_repo_file("plans/impl/external_dependency_tracker.md");
	let lower_content = content.to_ascii_lowercase();
	for required in ["IANA PEN", "CMVP", "custody-provider"] {
		assert!(
			lower_content.contains(&required.to_ascii_lowercase()),
			"external dependency tracker should mention `{required}`"
		);
	}

	let reserved_paths = [
		"plans/impl/external/iana_pen_filing_record.md",
		"plans/impl/external/cmvp_lab_contact_record.md",
		"plans/impl/external/custody_provider_onboarding_record.md",
	];
	for path in reserved_paths {
		assert!(
			content.contains(path),
			"tracker should reserve `{path}` as evidence path"
		);
	}
}

#[test]
fn claim_discipline_register_covers_sensitive_claim_domains() {
	let content = read_repo_file("plans/impl/claim_discipline.md");
	for required in [
		"Transport",
		"Custody",
		"Recovery",
		"Audit",
		"Compliance/FIPS",
		"Toron integration",
		"five-nines",
		"FIPS compliant",
	] {
		assert!(
			content.contains(required),
			"claim discipline register should cover `{required}`"
		);
	}
}
