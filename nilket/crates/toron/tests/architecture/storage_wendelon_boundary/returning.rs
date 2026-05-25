#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	ddl_admin::*, descriptor_invalidation::*, dml_delete_update::*, row_identity::*,
	wendelon_catalog::*,
};

#[test]
pub(crate) fn adr064_returning_supports_four_dml_variants() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");

	assert_eq!(
		planner
			.matches("lower_returning_projection(returning.as_deref())")
			.count(),
		3
	);
	for variant in ["Insert {", "InsertSelect {", "Update {", "Delete {"] {
		let body = logical
			.split(variant)
			.nth(1)
			.unwrap_or_else(|| panic!("missing {variant}"));
		assert!(body.contains("returning: Option<ReturningProjection>"));
	}
}

#[test]
pub(crate) fn adr064_ctas_returning_remains_out_of_scope() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let lower_create = brace_blocks_after_marker(&planner, "fn lower_create_table")
		.into_iter()
		.next()
		.expect("lower_create_table")
		.1;
	let ctas = logical
		.split("CreateTableAsSelect {")
		.nth(1)
		.expect("CTAS logical variant")
		.split("DropTable {")
		.next()
		.expect("CTAS logical body");

	assert!(!lower_create.contains("lower_returning_projection"));
	assert!(!ctas.contains("ReturningProjection"));
}

#[test]
pub(crate) fn adr064_sqlparser_upgrade_is_now_owned_by_adr073() {
	let manifest =
		std::fs::read_to_string(manifest_dir().join("Cargo.toml")).expect("read manifest");
	let architecture_test = architecture_corpus_source();

	assert!(manifest.contains("sqlparser = \"0.62\""));
	assert!(architecture_test.contains("fn adr073_sqlparser_is_pinned_to_062"));
	let stale_guard = ["fn ", "adr064_sqlparser", "_stays_054"].concat();
	assert!(!architecture_test.contains(&stale_guard));
}

#[test]
pub(crate) fn adr064_returning_ir_is_toron_local() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(logical.contains("pub enum ReturningProjection"));
	assert!(logical.contains("Exprs {\n\t\texprs: Vec<Expr>"));
	assert!(execution.contains("pub struct BoundReturningProjection"));
	assert!(!logical.contains("SelectItem"));
	assert!(!execution.contains("SelectItem"));
}

#[test]
pub(crate) fn adr064_bound_plan_schema_reports_returning_schema() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let schema = brace_blocks_after_marker(&execution, "pub fn schema(&self)")
		.into_iter()
		.next()
		.expect("ExecutionPlan::schema")
		.1;

	assert!(schema.contains("returning: Some(returning)"));
	assert!(schema.contains("returning.output_schema.clone()"));
	assert!(schema.contains("result_count_schema_int64(\"deleted\")"));
	assert!(schema.contains("Arc::new(Schema::empty())"));
}

#[test]
pub(crate) fn adr064_returning_projection_vocab_is_intentionally_narrow() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower = brace_blocks_after_marker(&planner, "fn lower_returning_projection")
		.into_iter()
		.next()
		.expect("lower_returning_projection")
		.1;

	for allowed in [
		"SelectItem::Wildcard(_)",
		"SelectItem::UnnamedExpr",
		"SelectItem::ExprWithAlias",
	] {
		assert!(lower.contains(allowed));
	}
	for rejected in [
		"mixed wildcard RETURNING",
		"qualified wildcard RETURNING",
		"qualified RETURNING columns",
	] {
		assert!(lower.contains(rejected));
	}
}

#[test]
pub(crate) fn adr064_returning_binds_against_target_user_schema() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let helper = brace_blocks_after_marker(&binder, "fn bind_returning_projection")
		.into_iter()
		.next()
		.expect("bind_returning_projection")
		.1;

	assert!(helper.contains(".fields()"));
	assert!(helper.contains("self.bind_expr_program(&exprs, schema).await?"));
	assert!(helper.contains("schema_for_exprs_with_output_names"));
	assert!(binder.contains("RETURNING column is reserved for storage metadata"));
}

#[test]
pub(crate) fn adr064_returning_does_not_touch_wendelon_or_worker_transport() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read wire");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");

	assert!(!wire.contains("Returning"));
	assert!(server.contains("ExecutionPlan::InsertSelect { .. } => \"insert_select\""));
	assert!(fragmenter.contains("InsertNotFragmentable"));
	assert!(fragmenter.contains("DeleteNotFragmentable"));
	assert!(fragmenter.contains("UpdateNotFragmentable"));
}

#[test]
pub(crate) fn adr064_returning_does_not_add_sql_wire_message_or_protocol_version() {
	let wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");
	let sql_tests = std::fs::read_to_string(manifest_dir().join("tests/sql_listener_test.rs"))
		.expect("read SQL listener tests");
	let message = brace_blocks_after_marker(&wire, "pub enum SqlMessage")
		.into_iter()
		.next()
		.expect("SqlMessage")
		.1;
	let protocol = brace_blocks_after_marker(&wire, "pub enum SqlProtocolVersion")
		.into_iter()
		.next()
		.expect("SqlProtocolVersion")
		.1;

	for variant in [
		"Execute(ExecuteRequest)",
		"Cancel(CancelRequest)",
		"ResultBatch(ResultBatch)",
		"Complete(Complete)",
		"Error(SqlError)",
	] {
		assert!(
			message.contains(variant),
			"missing SQL message variant {variant}"
		);
	}
	for forbidden in [
		"Returning",
		"ResultSchema",
		"MutationResult",
		"DmlResult",
		"SchemaCarrier",
	] {
		assert!(
			!message.contains(forbidden),
			"RETURNING added SQL wire frame surface {forbidden}"
		);
	}
	assert!(protocol.contains("Current"));
	assert!(!protocol.contains("V1"));
	assert!(!protocol.contains("V2"));
	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(daemon.contains("SqlMessage::ResultBatch(ResultBatch"));
	assert!(sql_tests.contains("sql_listener_dml_returning_sends_result_batches_then_complete"));
}

#[test]
pub(crate) fn adr064_returning_projection_is_validated_before_mutation() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_pos = coordinator
		.find("let returning_batches =\n\t\t\t\tproject_returning_batches")
		.expect("returning projection");
	let write_pos = coordinator
		.find(".write(WriteRequest")
		.expect("write request");

	assert!(insert_pos < write_pos);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(coordinator.contains("Executor::evaluate_bound_program_without_storage"));
}

#[test]
pub(crate) fn adr064_returning_rows_emit_only_after_storage_success() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let projection = coordinator
		.find("project_returning_batches(returning.as_ref()")
		.expect("projection");
	let write = coordinator[projection..]
		.find(".await?;")
		.map(|offset| projection + offset)
		.expect("storage await");
	let emit = coordinator[write..]
		.find("Ok(batches)")
		.map(|offset| write + offset)
		.expect("returning emit");

	assert!(projection < write);
	assert!(write < emit);
}

#[test]
pub(crate) fn adr064_complete_row_count_matches_storage_receipt_and_returned_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let daemon =
		std::fs::read_to_string(manifest_dir().join("src/daemon.rs")).expect("read daemon");

	assert!(coordinator.contains("verify_write_returning_receipt"));
	assert!(coordinator.contains("verify_delete_returning_receipt"));
	assert!(coordinator.contains("batch_row_count(batches)"));
	assert!(daemon.contains("batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64"));
}

#[test]
pub(crate) fn adr064_no_new_pk_gate_for_insert_or_delete_returning() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_insert = brace_blocks_after_marker(&binder, "async fn bind_insert(")
		.into_iter()
		.next()
		.expect("bind_insert")
		.1;
	let bind_delete = brace_blocks_after_marker(&binder, "async fn bind_delete(")
		.into_iter()
		.next()
		.expect("bind_delete")
		.1;

	assert!(bind_insert.contains("bind_returning_projection"));
	assert!(bind_delete.contains("bind_returning_projection"));
	assert!(!bind_insert.contains("PRIMARY KEY"));
	assert!(!bind_delete.contains("PRIMARY KEY"));
}

#[test]
pub(crate) fn adr064_update_returning_inherits_pk_update_contract() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update(")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("bind_returning_projection"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
}

#[test]
pub(crate) fn adr064_delete_returning_fails_closed_on_partial_receipt() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(coordinator.contains("DELETE RETURNING receipt mismatch"));
	assert!(coordinator.contains("outcome.already_deleted == 0"));
	assert!(coordinator.contains("outcome.not_found == 0"));
	assert!(delete_test.contains("delete_returning_fails_closed_on_partial_receipt"));
}

#[test]
pub(crate) fn adr064_delete_returning_uses_pre_delete_rows_without_identity() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let delete_mod = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete module");

	assert!(coordinator.contains("delete_candidate_user_batches(&candidate_batches, schema)"));
	assert!(coordinator.contains("project_returning_batches(Some(returning), &user_batches)"));
	assert!(delete_mod.contains("let identity_index = batch.num_columns() - 1"));
	assert!(delete_mod.contains("batch.columns()[..identity_index].to_vec()"));
}

#[test]
pub(crate) fn adr064_insert_returning_uses_inserted_target_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_test = std::fs::read_to_string(manifest_dir().join("tests/insert_test.rs"))
		.expect("read insert tests");

	assert!(coordinator.contains("write::materialize_insert_batch(schema, rows)"));
	assert!(coordinator.contains("std::slice::from_ref(&payload)"));
	assert!(insert_test.contains("insert_values_returning_emits_inserted_rows"));
}

#[test]
pub(crate) fn adr064_insert_select_returning_uses_target_shaped_payload() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let insert_test = std::fs::read_to_string(manifest_dir().join("tests/insert_test.rs"))
		.expect("read insert tests");

	assert!(coordinator.contains(
		"write::materialize_insert_select_batch(schema, target_columns, &source_batches)"
	));
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(insert_test.contains("insert_select_returning_preserves_target_reorder_and_coercion"));
}

#[test]
pub(crate) fn adr064_update_returning_uses_post_update_rows() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(
		coordinator.contains("write::materialize_update_select_batch(schema, &source_batches)")
	);
	assert!(coordinator.contains("project_returning_batches(returning.as_ref()"));
	assert!(update_test.contains("update_returning_emits_post_update_values"));
}

#[test]
pub(crate) fn adr064_returning_star_hides_storage_synthetic_columns() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let delete_test = std::fs::read_to_string(manifest_dir().join("tests/delete_test.rs"))
		.expect("read delete tests");

	assert!(binder.contains("ReturningProjection::Star"));
	assert!(binder.contains("schema.fields()"));
	assert!(delete_test.contains("delete_returning_star_hides_appended_row_identity"));
	assert!(delete_test.contains("batch.schema().fields().len(), 3"));
}

#[test]
pub(crate) fn adr064_returning_rejects_aggregate_window_subquery() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let lower = brace_blocks_after_marker(&planner, "fn lower_returning_projection")
		.into_iter()
		.next()
		.expect("lower_returning_projection")
		.1;

	assert!(lower.contains("expr_contains_aggregate_call"));
	assert!(lower.contains("expr_contains_window_function"));
	assert!(lower.contains("expr_contains_subquery"));
}

#[test]
pub(crate) fn adr064_descriptor_evolved_still_invalidates_without_retry() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("read coord");
	let update_test = std::fs::read_to_string(manifest_dir().join("tests/update_test.rs"))
		.expect("read update tests");

	assert!(coordinator.contains("invalidate_descriptor_evolution_on_error"));
	assert!(!coordinator.contains("retry_returning"));
	assert!(update_test.contains("update_descriptor_evolved_invalidates_without_retry"));
}

#[test]
pub(crate) fn adr064_prior_returning_absence_guards_are_retargeted() {
	let architecture_test = architecture_corpus_source();

	assert!(architecture_test.contains("adr062_returning_surface_is_retargeted_after_adr064"));
	assert!(
		architecture_test
			.contains("adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064")
	);
	let stale_delete = ["unsupported DELETE clause: ", "RETURNING"].concat();
	let stale_update = ["unsupported UPDATE clause: ", "RETURNING"].concat();
	assert!(!architecture_test.contains(&stale_delete));
	assert!(!architecture_test.contains(&stale_update));
}

#[test]
pub(crate) fn adr064_no_returning_specific_memory_or_config_knob() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let budget = std::fs::read_to_string(manifest_dir().join("src/distributed/budget.rs"))
		.expect("read budget");

	assert!(!config.to_lowercase().contains("returning"));
	assert!(!budget.to_lowercase().contains("returning"));
}

#[test]
pub(crate) fn adr064_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "clap", "tracing", "arrow"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr064_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"RETURNING",
		"ReturningProjection",
		"BoundReturningProjection",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained RETURNING surface {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr064_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"ReturningV1",
		"SqlReturningV1",
		"ReturningProjectionV1",
		"BoundReturningProjectionV1",
		"DmlReturningV1",
	]);
}

#[test]
pub(crate) fn adr064_prior_adr030_to_063_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr030_", "adr031_", "adr032_", "adr033_", "adr034_", "adr035_", "adr036_", "adr037_",
		"adr038_", "adr039_", "adr040_", "adr041_", "adr042_", "adr043_", "adr044_", "adr045_",
		"adr046_", "adr047_", "adr048_", "adr049_", "adr050_", "adr051_", "adr052_", "adr053_",
		"adr054_", "adr055_", "adr056_", "adr057_", "adr058_", "adr059_", "adr060_", "adr061_",
		"adr062_", "adr063_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains("adr062_returning_surface_is_retargeted_after_adr064"));
	assert!(
		architecture_test
			.contains("adr063_sql_response_is_complete_only_for_non_returning_dml_after_adr064")
	);
}
