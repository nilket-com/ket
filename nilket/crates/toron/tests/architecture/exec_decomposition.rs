use super::support::*;

const EXEC_ROOT_LINE_LIMIT: usize = 11_700;

#[test]
pub(crate) fn adr350_exec_root_declares_join_module() {
	let root = exec_root_source();

	assert!(
		root.contains("mod join;"),
		"exec root must declare the join semantic module"
	);
}

#[test]
pub(crate) fn adr350_exec_join_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join.rs");

	assert!(path.exists(), "missing exec/join.rs semantic module");
}

#[test]
pub(crate) fn adr350_exec_root_excludes_carved_join_clusters() {
	let root = exec_root_source();

	for carved_definition in [
		"async fn execute_hash_join(",
		"fn materialize_join_selection(",
		"struct SpilledJoinBuild",
		"struct SpilledInnerJoinBuild",
		"enum JoinOutputRow",
		"enum JoinKeyPlan",
		"fn join_spill_index_bytes(",
		"fn record_join_runtime_observation(",
	] {
		assert!(
			!root.contains(carved_definition),
			"exec root still defines carved join cluster marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr350_exec_root_is_bounded() {
	let lines = exec_root_source().lines().count();
	// ADR 350: first semantic executor decomposition ratchet. The post-join-carve root measured
	// 11,611 lines after fmt; 11,700 leaves import/module churn slack while failing if join logic
	// drifts back into exec.rs. Follow-up executor carves should lower this threshold.
	assert!(
		lines <= EXEC_ROOT_LINE_LIMIT,
		"exec root has {lines} lines; expected <= {EXEC_ROOT_LINE_LIMIT} after ADR 350 join carve"
	);
}

#[test]
pub(crate) fn adr350_exec_modules_have_no_query_number_identifiers() {
	let manifest = manifest_dir();
	let offenders = exec_source_files()
		.into_iter()
		.flat_map(|path| {
			std::fs::read_to_string(&path)
				.expect("read exec source file")
				.lines()
				.enumerate()
				.filter_map(|(idx, line)| {
					if contains_query_number_identifier(line) {
						Some((path.clone(), idx + 1, line.to_string()))
					} else {
						None
					}
				})
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"executor modules must stay shape-named, not TPC-H query-number branded:\n{}",
		format_offenders(&manifest, &offenders)
	);
}

#[test]
pub(crate) fn adr350_exec_public_surface_is_still_rooted() {
	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib source");
	let root = exec_root_source();
	let exec = exec_source();

	assert!(lib.contains("pub mod exec;"));
	assert!(root.contains("pub struct Executor"));
	assert!(root.contains("pub struct ExecutionContext"));
	assert!(exec.contains("pub(crate) fn materialize_scalar_rows("));
	let _ = std::mem::size_of::<toron::exec::Executor>();
	let _ = std::mem::size_of::<toron::exec::ExecutionContext>();
}

fn contains_query_number_identifier(line: &str) -> bool {
	line.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
		.any(|token| {
			let bytes = token.as_bytes();
			bytes.len() >= 3
				&& matches!(bytes[0], b'q' | b'Q')
				&& bytes[1].is_ascii_digit()
				&& bytes[2].is_ascii_digit()
		})
}
