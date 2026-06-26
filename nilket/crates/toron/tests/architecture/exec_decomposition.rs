use super::support::*;

const EXEC_ROOT_LINE_LIMIT: usize = 11_700;
const EXEC_JOIN_ROOT_LINE_LIMIT: usize = 5_900;
const EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT: usize = 3_800;

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
pub(crate) fn adr351_exec_join_root_declares_spill_module() {
	let join_root = exec_join_root_source();

	assert!(
		join_root.contains("mod spill;"),
		"exec/join root must declare the spill storage submodule"
	);
}

#[test]
pub(crate) fn adr351_exec_join_spill_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join/spill.rs");

	assert!(path.exists(), "missing exec/join/spill.rs storage module");
}

#[test]
pub(crate) fn adr352_exec_join_root_declares_streaming_module() {
	let join_root = exec_join_root_source();

	assert!(
		join_root.contains("mod streaming;"),
		"exec/join root must declare the streaming orchestration submodule"
	);
}

#[test]
pub(crate) fn adr352_exec_join_streaming_module_exists() {
	let manifest = manifest_dir();
	let path = manifest.join("src/exec/join/streaming.rs");

	assert!(
		path.exists(),
		"missing exec/join/streaming.rs orchestration module"
	);
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
pub(crate) fn adr351_exec_join_root_excludes_carved_spill_storage() {
	let join_root = exec_join_root_source();

	for carved_definition in [
		"struct SpilledJoinBuild",
		"struct SpilledInnerJoinBuild",
		"struct SpilledSemiAntiExistenceBuild",
		"struct JoinSpillPage",
		"fn write_join_spill_plaintext_page(",
		"fn encode_record_batch_ipc(",
		"fn join_spill_index_bytes(",
		"static NEXT_JOIN_SPILL_ID",
	] {
		assert!(
			!join_root.contains(carved_definition),
			"exec/join root still defines carved spill storage marker {carved_definition}"
		);
	}
}

#[test]
pub(crate) fn adr352_exec_join_root_excludes_carved_streaming_orchestration() {
	let join_root = exec_join_root_source();

	for carved_definition in [
		"struct ReorientedInnerJoinStage",
		"struct StreamingSemiAntiStage",
		"enum StreamingSemiAntiExistence",
		"struct StreamingLeftOuterRecombineRuntime",
		"fn extract_reoriented_inner_chain(",
		"fn decompose_streaming_existence_plan(",
		"fn extract_streaming_inner_prefix_semi_anti_chain(",
		"async fn try_streaming_join0_filter(",
		"async fn try_streaming_collapsing_global_aggregate(",
		"async fn prepare_streaming_semi_anti_runtime(",
		"fn probe_streaming_left_outer_recombine(",
	] {
		assert!(
			!join_root.contains(carved_definition),
			"exec/join root still defines carved streaming marker {carved_definition}"
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
pub(crate) fn adr351_exec_join_root_is_bounded() {
	let lines = exec_join_root_source().lines().count();
	// ADR 351: spill storage carved from exec/join.rs into exec/join/spill.rs. The post-carve
	// join root measured 5,801 lines after fmt; 5,900 leaves import/module churn slack while
	// failing if spill storage drifts back into the join root.
	assert!(
		lines <= EXEC_JOIN_ROOT_LINE_LIMIT,
		"exec/join root has {lines} lines; expected <= {EXEC_JOIN_ROOT_LINE_LIMIT} after ADR 351 spill carve"
	);
}

#[test]
pub(crate) fn adr352_exec_join_root_is_bounded_after_streaming_carve() {
	let lines = exec_join_root_source().lines().count();
	// ADR 352: streaming orchestration carved from exec/join.rs into exec/join/streaming.rs.
	// The post-carve join root measured 3,684 lines before fmt; 3,800 leaves import/module
	// churn slack while failing if streaming logic drifts back into the join root.
	assert!(
		lines <= EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT,
		"exec/join root has {lines} lines; expected <= {EXEC_JOIN_ROOT_AFTER_STREAMING_LINE_LIMIT} after ADR 352 streaming carve"
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
