use super::support::*;

const BINDER_MODULES: [&str; 7] = [
	"aggregate",
	"arithmetic",
	"explain",
	"join",
	"join_order",
	"pushdown",
	"selectivity",
];

#[test]
pub(crate) fn adr176_binder_source_concatenates_submodules() {
	let manifest = manifest_dir();
	let expected = [
		"src/binder.rs",
		"src/binder/aggregate.rs",
		"src/binder/arithmetic.rs",
		"src/binder/explain.rs",
		"src/binder/join.rs",
		"src/binder/join_order.rs",
		"src/binder/pushdown.rs",
		"src/binder/selectivity.rs",
		"src/binder/tests.rs",
	];
	let actual = binder_source_files()
		.into_iter()
		.map(|path| {
			path.strip_prefix(&manifest)
				.expect("binder source file under manifest")
				.to_string_lossy()
				.replace('\\', "/")
		})
		.collect::<Vec<_>>();

	assert_eq!(actual, expected);

	let support = std::fs::read_to_string(manifest.join("tests/architecture/support.rs"))
		.expect("read architecture support");
	assert_eq!(support.matches("pub(crate) fn binder_source()").count(), 1);
	assert!(support.contains("pub(crate) fn binder_source_files()"));
	assert!(support.contains("source_files(&module_root)"));
	assert!(support.contains("files.extend(production)"));
	assert!(support.contains("files.extend(tests)"));

	let binder = binder_source();
	for marker in [
		"struct Binder",
		"pub(super) fn aggregate_schema",
		"pub(super) fn bind_dimensioned_arithmetic",
		"pub(super) fn table_name",
		"pub(super) fn choose_join_strategy",
		"pub(super) fn analyze",
		"pub(super) fn predicate_pushdown_plan",
		"pub(super) fn predicate_includes_primary_key_equality",
		"fn binder_resolves_columns_to_ordinals",
	] {
		assert!(binder.contains(marker), "missing binder marker {marker}");
	}
}

#[test]
pub(crate) fn adr176_no_scattered_binder_file_reads() {
	let architecture_dir = manifest_dir().join("tests/architecture");
	let allowed = [
		architecture_dir.join("support.rs"),
		architecture_dir.join("binder_decomposition.rs"),
	];
	let offenders = source_lines(&architecture_dir)
		.into_iter()
		.filter(|(path, _, line)| {
			!allowed.iter().any(|allowed| allowed == path)
				&& ((line.contains("source(\"src/binder.rs\")")
					|| line.contains("join(\"src/binder.rs\")"))
					|| (line.contains("read_to_string") && line.contains("src/binder.rs")))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"architecture tests still read src/binder.rs directly:\n{}",
		format_offenders(&manifest_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr176_binder_root_excludes_carved_clusters() {
	let root = binder_root_source();
	assert!(root.contains("struct Binder"));
	assert!(root.contains("fn bind_plan_with_trace"));

	for carved in [
		"bind_aggregate_call",
		"aggregate_schema",
		"bind_dimensioned_arithmetic",
		"predicate_pushdown_plan",
		"choose_join_strategy",
		"predicate_includes_primary_key_equality",
		"table_name",
	] {
		assert!(
			!root.contains(&format!("fn {carved}(")),
			"binder root still defines carved cluster function {carved}"
		);
	}
}

#[test]
pub(crate) fn adr176_test_module_is_extracted() {
	let manifest = manifest_dir();
	let root = binder_root_source();
	let tests_path = manifest.join("src/binder/tests.rs");

	assert!(tests_path.exists(), "binder/tests.rs must exist");
	assert!(root.contains("#[cfg(test)]\nmod tests;"));
	assert!(
		!root.contains("mod tests {"),
		"binder root must not keep inline test module"
	);
}

#[test]
pub(crate) fn adr176_semantic_modules_exist() {
	let manifest = manifest_dir();
	let root = binder_root_source();

	for module in BINDER_MODULES {
		let path = manifest.join(format!("src/binder/{module}.rs"));
		assert!(path.exists(), "missing binder semantic module {module}");
		assert!(
			root.contains(&format!("mod {module};")),
			"binder root does not declare module {module}"
		);
	}
}

#[test]
pub(crate) fn adr176_binder_root_is_bounded() {
	let lines = binder_root_source().lines().count();
	assert!(
		lines <= 5_000,
		"binder root has {lines} lines; expected <= 5,000 after ADR176 split"
	);
}

#[test]
pub(crate) fn adr176_no_runtime_or_wire_surface_change() {
	let output = std::process::Command::new("git")
		.arg("-C")
		.arg(manifest_dir())
		.args(["status", "--porcelain", "--", "."])
		.output()
		.expect("run git status");
	assert!(
		output.status.success(),
		"git status failed: {}",
		String::from_utf8_lossy(&output.stderr)
	);

	let disallowed = String::from_utf8_lossy(&output.stdout)
		.lines()
		.filter_map(|line| line.get(3..))
		.map(|path| path.split(" -> ").last().unwrap_or(path))
		.filter(|path| {
			let adr179_runtime_evidence_surface = [
				"crates/toron/src/audit.rs",
				"crates/toron/src/coordinator.rs",
				"crates/toron/src/distributed/fragment.rs",
				"crates/toron/src/distributed/scheduler.rs",
				"crates/toron/src/distributed/task.rs",
				"crates/toron/src/exec.rs",
				"crates/toron/src/ir.rs",
				"crates/toron/src/lib.rs",
				"crates/toron/src/rune_engine.rs",
				"crates/toron/src/server/mod.rs",
				"crates/toron/src/sql_planner.rs",
				"crates/toron/tests/explain_analyze_test.rs",
				"crates/toron/tests/hash_join_kinds_test.rs",
				"crates/toron/tests/rune_relation_handles_test.rs",
			];
			*path != "crates/toron/src/binder.rs"
				&& !path.starts_with("crates/toron/src/binder/")
				&& !path.starts_with("crates/toron/tests/architecture/")
				&& !adr179_runtime_evidence_surface.contains(path)
		})
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>();

	assert!(
		disallowed.is_empty(),
		"ADR176 must not touch runtime, wire, storage, Wendelon, descriptor, optimizer-fact, or frontend surfaces: {disallowed:?}"
	);
}
