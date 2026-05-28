#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	ddl_admin::*, descriptor_invalidation::*, dml_delete_update::*, returning::*,
	wendelon_catalog::*,
};

#[test]
pub(crate) fn adr036_synthetic_identity_still_not_sql_visible() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");

	for source in [&planner, &binder] {
		assert!(source.contains("STORAGE_SYNTHETIC_COLUMN_NAMES"));
		assert!(source.contains("STORAGE_ROW_KEY_ID_COLUMN"));
		assert!(source.contains("STORAGE_ROW_HLC_COLUMN"));
	}
}

#[test]
pub(crate) fn adr037_task_execution_context_has_exact_field_set() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("read distributed context source");
	let task_context = brace_blocks_after_marker(&context, "pub struct TaskExecutionContext")
		.into_iter()
		.next()
		.expect("TaskExecutionContext body")
		.1;

	for field in [
		"pub session_id: SessionId",
		"pub tenant_id: TenantId",
		"pub read_bound: HlcBound",
		"pub trace_context: Option<TraceContext>",
	] {
		assert!(task_context.contains(field), "missing {field}");
	}
	for forbidden in [
		"query_id",
		"profile_hint",
		"runtime_filter",
		"row_identity",
		"task_budget",
		"task_telemetry",
		"legacy_table_pins",
		"join_spill",
	] {
		assert!(
			!task_context.contains(forbidden),
			"TaskExecutionContext must not carry {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr037_task_spec_carries_task_execution_context() {
	let task =
		std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs")).expect("task");
	let task_spec = brace_blocks_after_marker(&task, "pub struct TaskSpec")
		.into_iter()
		.next()
		.expect("TaskSpec body")
		.1;

	assert!(task.contains("use crate::distributed::context::TaskExecutionContext;"));
	assert!(task_spec.contains("pub context: TaskExecutionContext"));
}

#[test]
pub(crate) fn adr037_distributed_context_module_is_wired() {
	let module =
		std::fs::read_to_string(manifest_dir().join("src/distributed/mod.rs")).expect("mod");

	assert!(module.contains("pub mod context;"));
}

#[test]
pub(crate) fn adr037_fragment_config_carries_task_context() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let config = brace_blocks_after_marker(&fragmenter, "pub struct FragmentPlanConfig")
		.into_iter()
		.next()
		.expect("FragmentPlanConfig body")
		.1;

	assert!(config.contains("pub task_context: TaskExecutionContext"));
	assert!(fragmenter.contains("pub const fn with_task_context"));
}

#[test]
pub(crate) fn adr037_fragmenter_stamps_every_task_spec_with_context() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let task_specs = fragmenter.matches("TaskSpec {").count();
	let stamped = fragmenter.matches("context: config.task_context").count();

	assert!(task_specs > 0, "fragmenter should emit TaskSpec literals");
	assert_eq!(
		stamped, task_specs,
		"every fragment TaskSpec must be stamped"
	);
}

#[test]
pub(crate) fn adr037_worker_context_comes_from_task_spec_not_local_test() {
	let server = std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("server");
	let execute_task_wrapper =
		brace_blocks_after_marker(&server, "async fn execute_task_local_with_telemetry")
			.into_iter()
			.next()
			.expect("execute_task_local_with_telemetry body")
			.1;
	let execute_task = brace_blocks_after_marker(
		&server,
		"async fn execute_task_local_with_telemetry_and_stream_sink",
	)
	.into_iter()
	.next()
	.expect("execute_task_local_with_telemetry_and_stream_sink body")
	.1;

	assert!(execute_task_wrapper.contains("execute_task_local_with_telemetry_and_stream_sink"));
	assert!(execute_task.contains("ExecutionContext::new("));
	assert!(execute_task.contains("spec.context.session_id"));
	assert!(execute_task.contains("spec.identity.query_id"));
	assert!(execute_task.contains("spec.context.tenant_id"));
	assert!(execute_task.contains("spec.context.read_bound"));
	assert!(execute_task.contains("spec.context.trace_context"));
	assert!(execute_task.contains("crate::telemetry::task_trace_context(spec)"));
	assert!(!execute_task.contains("ExecutionContext::local_test()"));
}

#[test]
pub(crate) fn adr037_task_context_derives_from_execution_context_without_query_id() {
	let context = std::fs::read_to_string(manifest_dir().join("src/distributed/context.rs"))
		.expect("distributed context");
	let from_execution = brace_blocks_after_marker(&context, "pub fn from_execution_context")
		.into_iter()
		.next()
		.expect("from_execution_context body")
		.1;

	assert!(from_execution.contains("context.session_id"));
	assert!(from_execution.contains("context.tenant_id"));
	assert!(from_execution.contains("context.read_bound"));
	assert!(from_execution.contains("context.trace_context"));
	assert!(!from_execution.contains("context.query_id"));
}

#[test]
pub(crate) fn adr037_distributed_coordinator_is_context_and_sink_bearing() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let struct_body =
		brace_blocks_after_marker(&coordinator, "pub struct DistributedQueryCoordinator")
			.into_iter()
			.next()
			.expect("DistributedQueryCoordinator body")
			.1;

	assert!(struct_body.contains("context: ExecutionContext"));
	assert!(struct_body.contains("writer: Arc<dyn StorageWrite>"));
	assert!(struct_body.contains("deleter: Arc<dyn StorageDeleteRows>"));
	assert!(struct_body.contains("admin: Arc<dyn StorageAdmin>"));
}

#[test]
pub(crate) fn adr037_distributed_coordinator_exposes_write_sink_constructors() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("pub fn with_context("));
	assert!(coordinator.contains("pub fn with_writer("));
	assert!(coordinator.contains("pub fn with_writer_and_deleter("));
	assert!(coordinator.contains("pub fn with_context_writer_and_deleter("));
	assert!(coordinator.contains("pub fn with_storage_admin("));
	assert!(coordinator.contains("pub fn with_writer_deleter_and_admin("));
}

#[test]
pub(crate) fn adr037_distributed_dml_routes_to_shared_driver() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let execute_query =
		brace_blocks_after_marker(&coordinator, "async fn execute_query_with_admission")
			.into_iter()
			.find(|(_, block)| block.contains("DistributedMutationSourceExecutor"))
			.expect("distributed execute_query_with_admission body")
			.1;

	for variant in [
		"LogicalPlan::Insert",
		"LogicalPlan::InsertSelect",
		"LogicalPlan::CreateTableAsSelect",
		"LogicalPlan::Delete",
	] {
		assert!(execute_query.contains(variant), "missing {variant}");
	}
	assert!(execute_query.contains("DistributedMutationSourceExecutor"));
	assert!(execute_query.contains("execute_write_plan_with_source"));
}

#[test]
pub(crate) fn adr037_distributed_read_executor_wraps_fragment_scheduler() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let distributed_source = brace_blocks_after_marker(
		&coordinator,
		"impl<Registry, Transport> MutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("distributed source executor body")
	.1;

	assert!(distributed_source.contains("execute_bound_distributed_with_context("));
	assert!(distributed_source.contains("plan.clone()"));
	assert!(distributed_source.contains("self.context"));
	assert!(distributed_source.contains("self.query_budget"));
	assert!(distributed_source.contains(".await"));
}

#[test]
pub(crate) fn adr037_distributed_fragment_config_uses_execution_context() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let fragment_config = brace_blocks_after_marker(&coordinator, "fn fragment_config")
		.into_iter()
		.next()
		.expect("fragment_config body")
		.1;

	assert!(fragment_config.contains("context.query_id"));
	assert!(fragment_config.contains("TaskExecutionContext::from_execution_context"));
	assert!(fragment_config.contains("context"));
	assert!(fragment_config.contains("query_budget: QueryBudget"));
	assert!(fragment_config.contains("TaskBudget::from_query_budget(query_budget)"));
}

#[test]
pub(crate) fn adr037_mutation_source_executor_is_async_trait() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("#[async_trait::async_trait]\ntrait MutationSourceExecutor"));
	assert!(coordinator.contains("async fn execute_read_plan"));
	assert!(coordinator.contains("async fn execute_delete_candidate_plan"));
}

#[test]
pub(crate) fn adr037_local_mutation_source_wraps_executor() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let local_source = brace_blocks_after_marker(
		&coordinator,
		"impl MutationSourceExecutor for LocalMutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("local source executor body")
	.1;

	assert!(local_source.contains("self.executor"));
	assert!(local_source.contains("execute_bound(plan, None)"));
}

#[test]
pub(crate) fn adr037_local_delete_candidate_execution_clears_runtime_filter() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let local_source = brace_blocks_after_marker(
		&coordinator,
		"impl MutationSourceExecutor for LocalMutationSourceExecutor",
	)
	.into_iter()
	.next()
	.expect("local source executor body")
	.1;

	assert!(local_source.contains("with_runtime_filter(None)"));
	assert!(!local_source.contains("spec.context.runtime_filter"));
}

#[test]
pub(crate) fn adr037_shared_driver_handles_all_mutation_variants() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;

	for variant in [
		"ExecutionPlan::Insert",
		"ExecutionPlan::InsertSelect",
		"ExecutionPlan::CreateTableAsSelect",
		"ExecutionPlan::Delete",
	] {
		assert!(shared.contains(variant), "missing {variant}");
	}
}

#[test]
pub(crate) fn adr037_insert_values_does_not_execute_source_plan() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;
	let insert_arm = shared
		.split("ExecutionPlan::Insert {")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::InsertSelect").next())
		.expect("insert values arm");

	assert!(insert_arm.contains("materialize_insert_batch"));
	assert!(insert_arm.contains("write(WriteRequest"));
	assert!(!insert_arm.contains("execute_read_plan"));
	assert!(!insert_arm.contains("fragment_plan"));
}

#[test]
pub(crate) fn adr037_delete_candidate_plan_sets_row_identity_append() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let derive = brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
		.into_iter()
		.next()
		.expect("delete candidate builder")
		.1;

	assert!(derive.contains("ExecutionPlan::Scan"));
	assert!(derive.contains("projection: None"));
	assert!(derive.contains("row_identity: RowIdentityProjection::Append"));
}

#[test]
pub(crate) fn adr037_delete_candidate_plan_wraps_residual_filter() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let derive = brace_blocks_after_marker(&coordinator, "fn derive_delete_candidate_plan")
		.into_iter()
		.next()
		.expect("delete candidate builder")
		.1;

	assert!(derive.contains("if let Some(predicate) = predicate"));
	assert!(derive.contains("ExecutionPlan::Filter"));
	assert!(derive.contains("FilterDisposition::EvaluateInToron"));
}

#[test]
pub(crate) fn adr037_execution_scan_carries_row_identity_field() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("execution ir");
	let scan = brace_blocks_after_marker(&execution, "Scan {")
		.into_iter()
		.next()
		.expect("ExecutionPlan::Scan body")
		.1;

	assert!(scan.contains("row_identity: RowIdentityProjection"));
}

#[test]
pub(crate) fn adr037_scan_schema_appends_identity_only_when_requested() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("execution ir");
	let helper = brace_blocks_after_marker(&execution, "fn scan_output_schema")
		.into_iter()
		.next()
		.expect("scan_output_schema body")
		.1;

	assert!(helper.contains("RowIdentityProjection::None => schema.clone()"));
	assert!(helper.contains("RowIdentityProjection::Append"));
	assert!(helper.contains("STORAGE_ROW_KEY_ID_COLUMN"));
	assert!(helper.contains("DataType::FixedSizeBinary(32)"));
}

#[test]
pub(crate) fn adr037_binder_default_scans_use_no_row_identity() {
	let binder = std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("binder");
	let scan_plan = brace_blocks_after_marker(&binder, "fn scan_plan")
		.into_iter()
		.next()
		.expect("scan_plan body")
		.1;

	assert!(scan_plan.contains("row_identity: crate::storage::RowIdentityProjection::None"));
}

#[test]
pub(crate) fn adr037_executor_threads_scan_row_identity_to_storage() {
	let exec = std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("exec");
	let execute_bound = brace_blocks_after_marker(&exec, "pub fn execute_bound")
		.into_iter()
		.next()
		.expect("execute_bound body")
		.1;

	assert!(execute_bound.contains("row_identity,"));
	assert!(execute_bound.contains("row_identity: *row_identity"));
}

#[test]
pub(crate) fn adr037_shuffle_input_scans_do_not_request_row_identity() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let shuffle_scan = brace_blocks_after_marker(&fragmenter, "fn shuffle_input_scan")
		.into_iter()
		.next()
		.expect("shuffle_input_scan body")
		.1;

	assert!(shuffle_scan.contains("row_identity: crate::storage::RowIdentityProjection::None"));
}

#[test]
pub(crate) fn adr037_fragmenter_still_rejects_raw_mutation_plans() {
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("fragmenter");
	let fragment_plan = brace_blocks_after_marker(&fragmenter, "pub fn fragment_plan")
		.into_iter()
		.next()
		.expect("fragment_plan body")
		.1;

	for variant in [
		"ExecutionPlan::CreateTable",
		"ExecutionPlan::DropTable",
		"ExecutionPlan::Delete",
		"ExecutionPlan::Insert",
		"ExecutionPlan::InsertSelect",
		"ExecutionPlan::CreateTableAsSelect",
	] {
		assert!(fragment_plan.contains(variant), "missing {variant}");
	}
}

#[test]
pub(crate) fn adr037_old_distributed_dml_reject_strings_are_removed() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	for removed in [
		"distributed INSERT execution is not wired",
		"distributed INSERT INTO ... SELECT execution is not wired",
		"distributed CREATE TABLE AS SELECT execution is not wired",
		"distributed DELETE execution is not wired",
	] {
		assert!(
			!coordinator.contains(removed),
			"old reject remains: {removed}"
		);
	}
}

#[test]
pub(crate) fn adr037_functional_fixtures_cover_distributed_write_context() {
	let fixture =
		std::fs::read_to_string(manifest_dir().join("tests/distributed_write_context_test.rs"))
			.expect("distributed write context test");

	for fixture_name in [
		"distributed_insert_values_writes_without_dispatch",
		"distributed_insert_select_fragments_source_and_writes_once",
		"distributed_ctas_fragments_source_then_creates_and_writes",
		"distributed_delete_fragments_candidate_read_and_deletes_once",
	] {
		assert!(fixture.contains(fixture_name), "missing {fixture_name}");
	}
}

#[test]
pub(crate) fn adr035_scan_request_uses_row_identity_enum() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let scan_request = storage
		.split("pub struct ScanRequest")
		.nth(1)
		.and_then(|rest| rest.split("pub struct WriteRequest").next())
		.expect("ScanRequest block");

	assert!(storage.contains("pub enum RowIdentityProjection"));
	assert!(scan_request.contains("pub row_identity: RowIdentityProjection,"));
}

#[test]
pub(crate) fn adr035_no_include_row_key_id_bool() {
	for (path, line, text) in source_lines(&manifest_dir().join("src")) {
		assert!(
			!text.contains("include_row_key_id: bool"),
			"ADR 035 must use RowIdentityProjection, not a bool: {}:{line}",
			path.display()
		);
	}
}

#[test]
pub(crate) fn adr035_delete_request_uses_vec_identity() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DeleteRowsRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DeleteRowsRequest block");

	assert!(request.contains("pub row_key_ids: Vec<Identity>,"));
}

#[test]
pub(crate) fn adr035_delete_request_has_no_record_batch_payload() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let request = storage
		.split("pub struct DeleteRowsRequest")
		.nth(1)
		.and_then(|rest| rest.split("#[non_exhaustive]").next())
		.expect("DeleteRowsRequest block");

	assert!(!request.contains("RecordBatch"));
	assert!(!request.contains("payload"));
}

#[test]
pub(crate) fn adr035_delete_outcome_preserves_four_counters() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let outcome = storage
		.split("pub struct DeleteRowsOutcome")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ScanStream").next())
		.expect("DeleteRowsOutcome block");

	for field in [
		"pub requested: usize,",
		"pub deleted: usize,",
		"pub already_deleted: usize,",
		"pub not_found: usize,",
	] {
		assert!(outcome.contains(field), "missing ADR 035 counter {field}");
	}
}

#[test]
pub(crate) fn adr035_storage_delete_rows_trait_is_separate() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let trait_body = storage
		.split("pub trait StorageDeleteRows")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryStorageScan").next())
		.expect("StorageDeleteRows trait block");

	assert!(trait_body.contains("async fn delete_rows("));
	assert!(trait_body.contains("request: DeleteRowsRequest,"));
	assert!(trait_body.contains("Result<DeleteRowsOutcome, StorageError>"));
}

#[test]
pub(crate) fn adr035_storage_write_trait_unchanged() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_trait = storage
		.split("pub trait StorageWrite")
		.nth(1)
		.and_then(|rest| rest.split("pub trait StorageDeleteRows").next())
		.expect("StorageWrite trait block");

	assert!(write_trait.contains(
		"async fn write(&self, request: WriteRequest) -> Result<WriteOutcome, StorageError>;"
	));
	assert!(!write_trait.contains("delete_rows"));
}

#[test]
pub(crate) fn adr035_no_blake3_or_hash_identity_generation_in_toron() {
	let manifest_dir = manifest_dir();
	let sources = [
		manifest_dir.join("src/storage.rs"),
		manifest_dir.join("src/coordinator/delete.rs"),
	];

	for source_path in sources {
		let source = std::fs::read_to_string(&source_path).expect("read source");
		for forbidden in [
			"blake3",
			"Identity::blake3",
			"Identity::blake3_tenant",
			"Identity::blake3_keyed",
			"sha",
			"DefaultHasher",
		] {
			assert!(
				!source.contains(forbidden),
				"ADR 035 in-memory identities must be deterministic prefix/suffix, found {forbidden} in {}",
				source_path.display()
			);
		}
	}
}

#[test]
pub(crate) fn adr035_identity_scan_rejects_user_projection() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");

	assert!(storage.contains("request.row_identity == RowIdentityProjection::Append"));
	assert!(storage.contains("request.projected_columns.is_some()"));
	assert!(storage.contains("StorageError::Internal"));
	assert!(
		storage.contains("scan with row_identity=Append cannot be combined with projected_columns")
	);
}

#[test]
pub(crate) fn adr035_coordinator_delete_module_exists() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source");
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(coordinator.contains("mod delete;"));
	assert!(delete.contains("materialize_delete_row_key_ids"));
}

#[test]
pub(crate) fn adr035_materializer_validates_identity_column_shape() {
	let delete = std::fs::read_to_string(manifest_dir().join("src/coordinator/delete.rs"))
		.expect("read delete coordinator source");

	assert!(delete.contains("STORAGE_ROW_KEY_ID_COLUMN"));
	assert!(delete.contains("DataType::FixedSizeBinary(32)"));
	assert!(delete.contains("null_count() != 0"));
}

#[test]
pub(crate) fn adr035_select_scan_uses_row_identity_none() {
	let exec =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source");
	let scan_requests = brace_blocks_after_marker(&exec, "ScanRequest {");
	assert!(
		!scan_requests.is_empty(),
		"ADR 035 guard expected ScanRequest constructors"
	);
	for (line, block) in scan_requests {
		assert!(
			block.contains("RowIdentityProjection::None")
				|| block.contains("row_identity: *row_identity"),
			"ADR 035/037 ScanRequest at exec.rs:{line} must set row_identity explicitly"
		);
	}
}

#[test]
pub(crate) fn adr062_create_table_row_identity_type_is_toron_local() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(logical.contains("pub enum CreateTableRowIdentity"));
	assert!(logical.contains("AppendOnly"));
	assert!(logical.contains("PrimaryKey { column: String }"));
	assert!(!logical.contains("RowIdentityPolicy"));
	assert!(!execution.contains("RowIdentityPolicy"));
}

#[test]
pub(crate) fn adr062_logical_create_table_carries_row_identity() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let create = brace_blocks_after_marker(&logical, "CreateTable {")
		.into_iter()
		.next()
		.expect("logical CreateTable")
		.1;

	assert!(create.contains("row_identity: CreateTableRowIdentity,"));
}

#[test]
pub(crate) fn adr062_execution_create_table_carries_row_identity() {
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let create = brace_blocks_after_marker(&execution, "CreateTable {")
		.into_iter()
		.next()
		.expect("execution CreateTable")
		.1;

	assert!(create.contains("row_identity: CreateTableRowIdentity,"));
}

#[test]
pub(crate) fn adr062_binder_preserves_create_table_row_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");

	assert!(binder.contains("row_identity: row_identity.clone()"));
	assert!(binder.contains("row_identity,"));
}

#[test]
pub(crate) fn adr062_sql_planner_accepts_column_primary_key() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE pk_column (id INT NOT NULL PRIMARY KEY, note TEXT)")
		.expect("column PK should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
}

#[test]
pub(crate) fn adr062_sql_planner_accepts_table_primary_key() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE pk_table (id BIGINT NOT NULL, PRIMARY KEY(id))")
		.expect("table PK should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(
		row_identity,
		CreateTableRowIdentity::PrimaryKey {
			column: "id".to_string()
		}
	);
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_duplicate_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, PRIMARY KEY(id))")
		.expect_err("duplicate PK should reject");

	assert!(err.to_string().contains("PRIMARY KEY more than once"));
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_composite_primary_key() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (id INT NOT NULL, sku TEXT NOT NULL, PRIMARY KEY(id, sku))")
		.expect_err("composite PK should reject");

	assert!(
		err.to_string()
			.contains("PRIMARY KEY must reference exactly one column")
	);
}

#[test]
pub(crate) fn adr062_sql_planner_requires_primary_key_not_null() {
	for sql in [
		"CREATE TABLE t (id INT PRIMARY KEY)",
		"CREATE TABLE t (id INT, PRIMARY KEY(id))",
		"CREATE TABLE t (id INT NULL PRIMARY KEY)",
	] {
		let err = toron::sql_planner::SqlPlanner::new()
			.plan(sql)
			.expect_err("nullable PK should reject");
		assert!(
			err.to_string()
				.contains("PRIMARY KEY column must be declared NOT NULL")
		);
	}
}

#[test]
pub(crate) fn adr062_sql_planner_rejects_unsupported_primary_key_types() {
	let err = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE t (active BOOLEAN NOT NULL PRIMARY KEY)")
		.expect_err("unsupported PK type should reject");

	assert!(
		err.to_string()
			.contains("unsupported CREATE TABLE PRIMARY KEY column type")
	);
}

#[test]
pub(crate) fn adr062_ctas_has_no_primary_key_identity_surface() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");

	for source in [&logical, &execution] {
		let ctas = brace_blocks_after_marker(source, "CreateTableAsSelect {")
			.into_iter()
			.next()
			.expect("CTAS block")
			.1;
		assert!(!ctas.contains("row_identity"));
	}
	assert!(planner.contains("CTAS does not support explicit column lists"));
	assert!(planner.contains("unsupported CREATE TABLE clause: table-level PRIMARY KEY"));
}

#[test]
pub(crate) fn adr062_if_not_exists_compares_row_identity() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("matches_create_identity(&row_identity)"));
	assert!(catalog.contains("CatalogError::SchemaMismatch"));
	assert!(catalog.contains("TableRowIdentity::Unsupported"));
}

#[test]
pub(crate) fn adr062_no_pk_create_remains_append_only() {
	let plan = toron::sql_planner::SqlPlanner::new()
		.plan("CREATE TABLE no_pk (id INT NOT NULL)")
		.expect("no-PK create should plan");

	let LogicalPlan::CreateTable { row_identity, .. } = plan else {
		panic!("expected create table");
	};
	assert_eq!(row_identity, CreateTableRowIdentity::AppendOnly);
}

#[test]
pub(crate) fn adr062_update_surface_is_present_under_pk_update_contract_after_adr063() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let fragmenter = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");

	assert!(planner.contains("Statement::Update(update) => self.lower_update(update)"));
	assert!(logical.contains("Update {"));
	assert!(execution.contains("Update {"));
	assert!(planner.contains("lower_returning_projection(returning.as_deref())"));
	assert!(binder.contains("UPDATE requires a PRIMARY KEY table"));
	assert!(binder.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(fragmenter.contains("UpdateNotFragmentable"));
	assert!(server.contains("ExecutionPlan::Update { .. } => \"update\""));
}

#[test]
pub(crate) fn adr062_returning_surface_is_retargeted_after_adr064() {
	let planner =
		std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs")).expect("read planner");
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let execution =
		std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs")).expect("read exec IR");

	assert!(planner.contains("fn lower_returning_projection"));
	assert!(logical.contains("enum ReturningProjection"));
	assert!(execution.contains("struct BoundReturningProjection"));
	assert!(!planner.contains("unsupported CREATE TABLE AS clause: RETURNING"));
}

#[test]
pub(crate) fn adr062_prior_adr038_append_only_guard_retargeted() {
	let architecture_test = architecture_corpus_source();

	assert!(architecture_test.contains(
		"adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062"
	));
	let old_guard = [
		"fn ",
		"adr038_wendelon_create_uses_append_only",
		"_row_identity_policy",
	]
	.concat();
	assert!(!architecture_test.contains(&old_guard));
	assert!(architecture_test.contains("adr062_no_pk_create_remains_append_only"));
}

#[test]
pub(crate) fn adr062_nilket_root_stays_runtime_free() {
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
pub(crate) fn adr062_fixture_worker_source_unchanged() {
	let worker = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker");

	for forbidden in [
		"CreateTableRowIdentity",
		"TableRowIdentity",
		"PrimaryKeyDescriptorV1",
		"PRIMARY KEY",
	] {
		assert!(
			!worker.contains(forbidden),
			"fixture worker gained {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr062_new_surface_is_brand_clean_and_has_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"CreateTableRowIdentityV1",
		"TableRowIdentityV1",
		"PrimaryKeyDdlV1",
		"CreateTablePrimaryKeyV1",
		"CreateTableRowIdentityPolicyV1",
	]);
}

#[test]
pub(crate) fn adr062_prior_adr033_to_061_guards_continue_to_pass_except_retarget() {
	let architecture_test = architecture_corpus_source();

	for prefix in [
		"adr033_", "adr034_", "adr035_", "adr036_", "adr037_", "adr038_", "adr039_", "adr040_",
		"adr041_", "adr042_", "adr043_", "adr044_", "adr045_", "adr046_", "adr047_", "adr048_",
		"adr049_", "adr050_", "adr051_", "adr052_", "adr053_", "adr054_", "adr055_", "adr056_",
		"adr057_", "adr058_", "adr059_", "adr060_", "adr061_",
	] {
		let count = architecture_test.matches(&format!("fn {prefix}")).count();
		assert!(count > 0, "missing prior-cycle guard prefix {prefix}");
	}
	assert!(architecture_test.contains(
		"adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062"
	));
}

#[test]
pub(crate) fn adr063_binder_requires_primary_key_row_identity() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("TableRowIdentity::PrimaryKey"));
	assert!(bind_update.contains("primary_key_columns.contains(primary_key_ordinal)"));
	assert!(bind_update.contains("UPDATE requires a PRIMARY KEY table"));
}

#[test]
pub(crate) fn adr063_binder_rejects_primary_key_assignment() {
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder");
	let bind_update = brace_blocks_after_marker(&binder, "async fn bind_update")
		.into_iter()
		.next()
		.expect("bind_update")
		.1;

	assert!(bind_update.contains("UPDATE cannot modify the PRIMARY KEY column"));
	assert!(bind_update.contains("STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column.as_str())"));
}

#[test]
pub(crate) fn adr035_synthetic_names_remain_rejected_in_sql() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let binder =
		std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source");

	for source in [&planner, &binder] {
		assert!(source.contains("STORAGE_SYNTHETIC_COLUMN_NAMES"));
		assert!(source.contains("STORAGE_ROW_KEY_ID_COLUMN"));
		assert!(source.contains("STORAGE_ROW_HLC_COLUMN"));
	}
}
