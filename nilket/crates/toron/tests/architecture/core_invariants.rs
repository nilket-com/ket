#![allow(unused_imports)]

use super::support::*;
use super::{
	distributed_execution::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, rune_frontend::*, sql_planner_surface::*,
	storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn no_eval_time_string_lookup() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("column_by_name"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"RecordBatch::column_by_name found outside binding code:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn no_public_result_string_boundaries() {
	let manifest_dir = manifest_dir();
	let result_marker = ["Result", "<"].concat();
	let string_marker = ["String", ">"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("pub ") && line.contains(&result_marker) && line.contains(&string_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"public Result<_, String> boundaries found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn bound_expr_node_has_no_string_column_variant() {
	let source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(!source.contains("Column(String)"));
	assert!(source.contains("Column(ResolvedColumn)"));
}

#[test]
pub(crate) fn scan_stream_collection_localized() {
	let manifest_dir = manifest_dir();
	let try_collect_marker = ["try", "_", "collect"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			if relative == Path::new("src/coordinator.rs") || relative == Path::new("src/error.rs")
			{
				return false;
			}
			line.contains("ScanStream::collect")
				|| line.contains(".batches.collect()")
				|| line.contains(&try_collect_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ScanStream collection outside coordinator boundary:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn architecture_guard_rune_context_is_allow_listed() {
	let source = std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source");
	let default_modules_marker = ["with", "_", "default", "_", "modules"].concat();

	assert!(!source.contains(&default_modules_marker));
	assert!(source.contains("Context::new()"));
	assert!(source.contains("install_minimal_primitives"));
}

#[test]
pub(crate) fn architecture_guards_unchanged_plus_sort_variant() {
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let logical_plan = logical_source
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");
	assert_eq!(enum_variant_count(logical_plan), 29);
	assert!(logical_plan.contains("Sort {"));
	assert!(logical_plan.contains("Take {"));
	assert!(logical_plan.contains("Distinct {"));
	assert!(logical_plan.contains("Union {"));
	assert!(logical_plan.contains("SetOpAll {"));
	assert!(logical_plan.contains("Repeat {"));
	assert!(logical_plan.contains("Explain {"));
	assert!(logical_plan.contains("ExplainAnalyze {"));
	assert!(logical_plan.contains("CreateTable {"));
	assert!(logical_plan.contains("CreateTableAsSelect {"));
	assert!(logical_plan.contains("CreateLogicalType {"));
	assert!(logical_plan.contains("DropTable {"));
	assert!(logical_plan.contains("Update {"));
	assert!(logical_plan.contains("Insert {"));
	assert!(logical_plan.contains("InsertSelect {"));
	assert!(logical_plan.contains("output_names: Vec<Option<String>>"));
	assert!(logical_source.contains("pub struct SortExpr"));
	assert!(logical_source.contains("pub enum InsertLiteral"));

	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	assert_eq!(enum_variant_count(execution_plan), 23);
	assert!(execution_plan.contains("EmptyRelation {"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("Union {"));
	assert!(execution_plan.contains("Repeat {"));
	assert!(execution_plan.contains("Sort {"));
	assert!(execution_plan.contains("Take {"));
	assert!(execution_plan.contains("CreateTable {"));
	assert!(execution_plan.contains("CreateTableAsSelect {"));
	assert!(execution_plan.contains("CreateLogicalType {"));
	assert!(execution_plan.contains("DropTable {"));
	assert!(execution_plan.contains("Update {"));
	assert!(execution_plan.contains("Insert {"));
	assert!(execution_plan.contains("InsertSelect {"));
	assert!(execution_plan.contains("predicate: BoundExprProgram"));
	assert!(execution_plan.contains("exprs: BoundExprProgram"));
	assert!(execution_plan.contains("group_expr: BoundExprProgram"));
	assert!(execution_plan.contains("aggr_expr: Vec<BoundAggregateCall>"));
	assert!(execution_plan.contains("key: BoundExprProgram"));
	assert!(execution_source.contains("pub struct ExecutionSortKey"));
}

#[test]
pub(crate) fn prior_adr04x_architecture_guards_pass_post_http_extraction() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_",
		"adr048_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing ADR guard prefix {prefix}");
	}
	for required in [
		"toron_observability_ready_metrics_preserve_responses_post_extraction",
		"toron_inspect_preserves_ready_fetch_post_extraction",
		"prior_adr04x_architecture_guards_pass_post_http_extraction",
	] {
		assert!(
			architecture_test.contains(required)
				|| std::fs::read_to_string(
					manifest_dir().join("tests/observability_listener_test.rs")
				)
				.expect("read observability listener test")
				.contains(required)
				|| std::fs::read_to_string(manifest_dir().join("src/cli.rs"))
					.expect("read cli source")
					.contains(required),
			"ADR 048 preservation fixture {required} must remain present"
		);
	}
}

#[test]
pub(crate) fn adr076_architecture_test_is_only_a_harness() {
	let harness = std::fs::read_to_string(manifest_dir().join("tests/architecture_test.rs"))
		.expect("read architecture harness");

	assert!(!harness.contains("#[test]"));
	assert!(!harness.contains("#[tokio::test]"));
	for line in harness
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty())
	{
		assert!(
			line == "mod architecture;" || line.starts_with("#![") || line.starts_with("//"),
			"unexpected architecture_test.rs harness line: {line}"
		);
	}
	assert!(harness.contains("mod architecture;"));
}

#[test]
pub(crate) fn adr076_architecture_modules_are_semantic_not_adr_numbered() {
	let architecture_dir = manifest_dir().join("tests/architecture");
	let offenders = std::fs::read_dir(&architecture_dir)
		.expect("read architecture dir")
		.map(|entry| entry.expect("read architecture entry").path())
		.filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("rs"))
		.filter_map(|path| {
			path.file_name()
				.and_then(|name| name.to_str())
				.map(str::to_owned)
		})
		.filter(|name| {
			name.starts_with("adr_")
				|| name
					.strip_prefix("adr")
					.and_then(|rest| rest.chars().next())
					.is_some_and(|ch| ch.is_ascii_digit())
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"architecture modules must be semantic, not ADR-numbered: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr076_corpus_content_guards_read_the_architecture_corpus_not_the_harness() {
	let core =
		std::fs::read_to_string(manifest_dir().join("tests/architecture/core_invariants.rs"))
			.expect("read core architecture guards");
	let mut scrubbed_core = core.clone();
	for marker in [
		"fn adr076_architecture_test_is_only_a_harness",
		"fn adr076_corpus_content_guards_read_the_architecture_corpus_not_the_harness",
	] {
		for (_, block) in brace_blocks_after_marker(&core, marker) {
			scrubbed_core = scrubbed_core.replace(&block, "");
		}
	}
	assert!(
		!scrubbed_core.contains("manifest_dir().join(\"tests/architecture_test.rs\")"),
		"only the ADR076 harness guard may read architecture_test.rs"
	);

	for (path, _, line) in source_lines(&manifest_dir().join("tests/architecture")) {
		if path.ends_with("support.rs") || path.ends_with("core_invariants.rs") {
			continue;
		}
		assert!(
			!line.contains("manifest_dir().join(\"tests/architecture_test.rs\")")
				|| line.contains("wendelon_manifest_dir().join"),
			"content guard reads harness instead of corpus: {}",
			path.display()
		);
	}
	assert!(architecture_corpus_source().contains("fn adr075_"));
}

#[test]
pub(crate) fn adr076_existing_adr_guard_leaf_names_are_preserved() {
	let architecture = architecture_corpus_source();
	for guard in [
		"fn adr014_sql_in_absence_guard_is_retargeted_after_adr074",
		"fn adr031_bound_expr_program_shape",
		"fn adr048_prior_adr04x_guards_continue_to_pass",
		"fn adr065_flat_observability_http_runtime_crate_removed",
		"fn adr068_external_context_fields_are_shared_ring_neutral_and_stripped",
		"fn adr073_sqlparser_is_pinned_to_062",
		"fn adr075_partial_pushdown_splits_bound_top_level_and_only",
	] {
		assert!(
			architecture.contains(guard),
			"missing preserved guard {guard}"
		);
	}
}

#[test]
pub(crate) fn adr076_support_helpers_stay_test_local() {
	let support_path = manifest_dir().join("tests/architecture/support.rs");
	let support = std::fs::read_to_string(&support_path).expect("read architecture support");

	assert!(support.contains("pub(crate) fn architecture_corpus_source"));
	assert!(support.contains("pub(crate) struct RecordingStorageScan"));
	for source_path in source_files(&manifest_dir().join("src")) {
		let source = std::fs::read_to_string(&source_path).expect("read production source");
		assert!(
			!source.contains("architecture_corpus_source"),
			"architecture support helper leaked into production source {}",
			source_path.display()
		);
	}
}

#[test]
pub(crate) fn adr076_architecture_command_remains_stable() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let harness_path = manifest_dir().join("tests").join("architecture_test.rs");

	assert!(harness_path.exists());
	assert!(!manifest.contains("[[test]]"));
	assert!(!manifest.contains("architecture_guard"));
}

#[test]
pub(crate) fn adr076_no_production_runtime_or_dependency_change() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read Toron manifest");
	let workspace_manifest = std::fs::read_to_string(workspace_root_dir().join("Cargo.toml"))
		.expect("read workspace manifest");

	for (name, source) in [
		("Toron manifest", manifest.as_str()),
		("workspace manifest", workspace_manifest.as_str()),
	] {
		assert!(
			!source
				.lines()
				.map(str::trim_start)
				.any(|line| line.starts_with("syn ") || line.starts_with("syn=")),
			"{name} gained a direct syn dependency"
		);
	}
	for path in source_files(&manifest_dir().join("src")) {
		let source = std::fs::read_to_string(&path).expect("read production source");
		assert!(
			!source.contains("architecture_corpus_source"),
			"ADR076 architecture corpus helper leaked into {}",
			path.display()
		);
	}
}

#[test]
pub(crate) fn adr077_no_architecture_module_exceeds_line_ceiling() {
	let manifest_dir = manifest_dir();
	let architecture_dir = manifest_dir.join("tests/architecture");
	let offenders = source_files(&architecture_dir)
		.into_iter()
		.filter(|path| path.file_name().and_then(|name| name.to_str()) != Some("support.rs"))
		.filter_map(|path| {
			let line_count = std::fs::read_to_string(&path)
				.expect("read architecture module")
				.lines()
				.count();
			(line_count > 2500).then_some((path, line_count))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"architecture modules exceed 2500-line ceiling:\n{}",
		offenders
			.iter()
			.map(|(path, line_count)| format!(
				"{}: {line_count}",
				path.strip_prefix(&manifest_dir).unwrap_or(path).display()
			))
			.collect::<Vec<_>>()
			.join("\n")
	);
}

#[test]
pub(crate) fn adr077_second_level_modules_are_semantic_not_adr_numbered() {
	let architecture_dir = manifest_dir().join("tests/architecture");
	for flat_jumbo in ["distributed_execution.rs", "storage_wendelon_boundary.rs"] {
		assert!(
			!architecture_dir.join(flat_jumbo).exists(),
			"ADR077 jumbos must be directory modules, not flat files: {flat_jumbo}"
		);
	}
	for expected in [
		"distributed_execution/mod.rs",
		"distributed_execution/tenant_admission.rs",
		"distributed_execution/descriptor_sync.rs",
		"distributed_execution/cancellation.rs",
		"distributed_execution/sql_ingress.rs",
		"distributed_execution/worker_lifecycle.rs",
		"distributed_execution/scheduler_dispatch.rs",
		"distributed_execution/shuffle_lifecycle.rs",
		"distributed_execution/sql_file_execution.rs",
		"distributed_execution/resource_governance.rs",
		"storage_wendelon_boundary/mod.rs",
		"storage_wendelon_boundary/ddl_admin.rs",
		"storage_wendelon_boundary/row_identity.rs",
		"storage_wendelon_boundary/dml_delete_update.rs",
		"storage_wendelon_boundary/returning.rs",
		"storage_wendelon_boundary/descriptor_invalidation.rs",
		"storage_wendelon_boundary/wendelon_catalog.rs",
	] {
		assert!(
			architecture_dir.join(expected).exists(),
			"missing ADR077 semantic sub-bucket {expected}"
		);
	}

	let offenders = source_files(&architecture_dir)
		.into_iter()
		.filter_map(|path| {
			let relative = path.strip_prefix(&architecture_dir).ok()?;
			if relative.components().count() < 2 {
				return None;
			}
			path.file_name()
				.and_then(|name| name.to_str())
				.map(str::to_owned)
		})
		.filter(|name| {
			name.starts_with("adr_")
				|| name
					.strip_prefix("adr")
					.and_then(|rest| rest.chars().next())
					.is_some_and(|ch| ch.is_ascii_digit())
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"second-level architecture modules must be semantic, not ADR-numbered: {offenders:?}"
	);
}

#[test]
pub(crate) fn adr077_architecture_corpus_source_recurses_subdirectories() {
	let architecture_dir = manifest_dir().join("tests/architecture");
	let files = source_files(&architecture_dir);
	for expected in [
		"distributed_execution/tenant_admission.rs",
		"distributed_execution/scheduler_dispatch.rs",
		"storage_wendelon_boundary/ddl_admin.rs",
		"storage_wendelon_boundary/returning.rs",
	] {
		assert!(
			files.iter().any(|path| path.ends_with(expected)),
			"architecture source_files must include recursive sub-bucket {expected}"
		);
	}

	let corpus = architecture_corpus_source();
	for guard in [
		"fn adr049_sql_tenant_source_is_authenticated_peer_identity",
		"fn scheduler_dispatch_loop_uses_acceptance_barrier",
		"fn adr029_sql_planner_accepts_only_select_and_create_table",
		"fn adr064_returning_supports_four_dml_variants",
	] {
		assert!(
			corpus.contains(guard),
			"architecture corpus must include sub-bucket guard {guard}"
		);
	}
}

#[test]
pub(crate) fn adr077_architecture_command_remains_stable() {
	let manifest_dir = manifest_dir();
	let manifest =
		std::fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read Toron manifest");
	let harness = std::fs::read_to_string(manifest_dir.join("tests/architecture_test.rs"))
		.expect("read architecture harness");
	let architecture_mod = std::fs::read_to_string(manifest_dir.join("tests/architecture/mod.rs"))
		.expect("read architecture module declaration");

	assert!(harness.contains("mod architecture;"));
	assert!(architecture_mod.contains("pub(crate) mod distributed_execution;"));
	assert!(architecture_mod.contains("pub(crate) mod storage_wendelon_boundary;"));
	assert!(!manifest.contains("[[test]]"));
	assert!(!manifest.contains("architecture_guard"));
}

#[test]
pub(crate) fn adr077_leaf_names_conserved_after_subbucketing() {
	let architecture = architecture_corpus_source();
	for guard in [
		"fn distributed_identity_keeps_morsel_id_worker_local",
		"fn adr049_sql_tenant_source_is_authenticated_peer_identity",
		"fn adr053_descriptor_sync_lives_in_toron_consumer",
		"fn adr059_reuses_existing_cancel_wire_and_rejects_control_protocol_drift",
		"fn adr029_sql_planner_accepts_only_select_and_create_table",
		"fn adr035_scan_request_uses_row_identity_enum",
		"fn adr063_sql_planner_accepts_statement_update",
		"fn adr064_returning_supports_four_dml_variants",
	] {
		assert!(
			architecture.contains(guard),
			"missing preserved ADR077 sub-bucket leaf {guard}"
		);
	}
	let adr077_marker = ["fn", "adr077_"].join(" ");
	assert_eq!(architecture.matches(&adr077_marker).count(), 5);
}

#[test]
pub(crate) fn adr462_query_budget_has_no_silent_default() {
	// The 64 MiB unit-test budget must be an explicit choice at every site:
	// a Default impl let it reach production silently (measured by ADR 454-461).
	assert_toron_sources_do_not_contain(&[
		"impl Default for QueryBudget",
		"QueryBudget::default()",
	]);
}

#[test]
pub(crate) fn adr463_daemon_coordinator_budget_is_config_owned() {
	// The coordinator budget must flow through the config-owned constructor;
	// a bare constant at the daemon site would silently orphan the knobs.
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("daemon");
	assert!(
		daemon.contains("config.worker.query_budget()"),
		"daemon coordinator budget must come from ToronWorkerConfig::query_budget()"
	);
}
