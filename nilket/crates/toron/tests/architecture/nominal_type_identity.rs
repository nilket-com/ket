#![allow(unused_imports)]

use super::support::*;

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

fn execution_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source")
}

fn exec_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec source")
}

fn pbc_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/pbc.rs")).expect("read pbc source")
}

fn aggregate_kernel_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/aggregate_kernel.rs"))
		.expect("read aggregate kernel source")
}

fn binder_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder source")
}

fn catalog_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog source")
}

fn coordinator_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator source")
}

fn error_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error source")
}

fn rune_engine_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source")
}

fn wendelon_table_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema source")
}

fn wendelon_compute_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/integration/compute.rs"))
		.expect("read Wendelon compute integration source")
}

fn wendelon_raft_storage_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/consensus/raft_storage.rs"))
		.expect("read Wendelon raft storage source")
}

fn wendelon_control_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/network/control_server.rs"))
		.expect("read Wendelon control server source")
}

fn wendelon_client_core_source() -> String {
	std::fs::read_to_string(wendelon_manifest_dir().join("src/client/core.rs"))
		.expect("read Wendelon client core source")
}

fn wendelon_client_lib_source() -> String {
	std::fs::read_to_string(
		wendelon_manifest_dir()
			.join("crates")
			.join("wendelon-client")
			.join("src")
			.join("lib.rs"),
	)
	.expect("read Wendelon client lib source")
}

fn storage_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/storage.rs")).expect("read storage source")
}

fn sql_planner_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read sql planner source")
}

fn source_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing source block for {marker}"))
		.1
}

#[test]
pub(crate) fn adr087_logical_type_id_is_sidecar_not_arrow_replacement() {
	let ir = ir_source();
	let execution = execution_source();

	assert!(ir.contains("pub struct LogicalTypeId(pub u32);"));
	assert!(ir.contains("pub struct LogicalTypeSpec"));
	assert!(ir.contains("physical: DataType"));
	assert!(execution.contains("pub node_types: Vec<DataType>"));
	assert!(execution.contains("pub node_logical_types: Vec<LogicalTypeId>"));
	assert!(!ir.contains("pub struct DataType"));
}

#[test]
pub(crate) fn adr087_bound_expr_program_tracks_logical_type_arity() {
	let execution = execution_source();
	let program = source_block(&execution, "pub struct BoundExprProgram");
	let validate = source_block(&execution, "pub fn validate");

	assert!(program.contains("pub node_logical_types: Vec<LogicalTypeId>"));
	assert!(execution.contains("NodeLogicalTypeArityMismatch"));
	assert!(validate.contains("self.nodes.len() != self.node_logical_types.len()"));
}

#[test]
pub(crate) fn adr087_logical_ascription_has_no_bound_runtime_node() {
	let ir = ir_source();
	let execution = execution_source();
	let bound_node = source_block(&execution, "pub enum BoundExprNode");

	assert!(ir.contains("LogicalAscription {"));
	assert!(!bound_node.contains("LogicalAscription"));
}

#[test]
pub(crate) fn adr087_sidecar_preserved_through_clone_and_merge() {
	let binder = binder_source();
	let clone = source_block(&binder, "fn clone_bound_subgraph");
	let merge = source_block(&binder, "fn merge_aggregate_args");

	assert!(clone.contains("source.node_logical_type(id)"));
	assert!(clone.contains("builder.set_node_logical_type(cloned, logical_type)"));
	assert!(merge.contains("node_logical_types: Vec::new()"));
	assert!(merge.contains("merged.node_logical_types.extend(program.node_logical_types)"));
}

#[test]
pub(crate) fn adr087_nominal_comparison_gate_after_physical_coercion() {
	let binder = binder_source();
	let comparison = source_block(&binder, "fn bind_comparison");
	let coerce = comparison
		.find("coerce_comparison_literals")
		.expect("comparison literal coercion");
	let non_null_branch = &comparison[coerce..];
	let physical_check = non_null_branch
		.find("if left_type != right_type")
		.expect("physical comparison type gate");
	let nominal = non_null_branch
		.find("nominal_compatible")
		.expect("nominal comparison gate");

	assert!(physical_check < nominal);
}

#[test]
pub(crate) fn adr087_nominal_in_list_gate_reuses_shared_helper() {
	let binder = binder_source();
	let in_list = source_block(&binder, "fn bind_in_list");

	assert_eq!(binder.matches("fn nominal_compatible").count(), 1);
	assert!(in_list.contains("nominal_compatible("));
}

#[test]
pub(crate) fn adr110_case_preserves_compatible_logical_identity() {
	let binder = binder_source();
	let bind_case = source_block(&binder, "fn bind_case");
	let case_output = source_block(&binder, "fn case_output_logical_type");

	assert!(
		bind_case.contains("case_output_logical_type(builder, &typed_result_ids, &output_type)?")
	);
	let append_case = bind_case
		.find("builder.append_case")
		.expect("CASE node append");
	let stamp = bind_case
		.find("set_node_logical_type(case_id, output_logical_type)")
		.expect("CASE logical identity stamp");
	assert!(append_case < stamp);

	assert_eq!(binder.matches("fn nominal_compatible").count(), 1);
	for marker in [
		"node_type",
		"node_logical_type",
		"builder.literal",
		"nominal_compatible(",
		"branch_type != *output_type",
		"cast explicitly to erase identity before widening",
	] {
		assert!(case_output.contains(marker), "missing CASE marker {marker}");
	}
	assert!(!case_output.contains("is_case_null_placeholder"));
	assert!(!case_output.contains("reconcile_identity"));
}

#[test]
pub(crate) fn adr087_nominal_join_key_gate_extends_existing_type_gate() {
	let binder = binder_source();
	let validate = source_block(&binder, "fn validate_join_key_types");

	assert!(binder.contains("struct ResolvedJoinKey"));
	assert!(binder.contains("logical_type: LogicalTypeId"));
	assert!(validate.contains("JoinKeyTypeMismatch"));
	assert!(validate.contains("JoinKeyTypeUnsupported"));
	assert!(validate.contains("nominal_compatible("));
}

#[test]
pub(crate) fn adr087_nominal_mismatch_has_dedicated_error() {
	let error = error_source();

	assert!(error.contains("NominalTypeMismatch"));
	assert!(error.contains("nominal type mismatch: {left}({physical}) vs {right}({physical})"));
}

#[test]
pub(crate) fn adr087_rune_surface_uses_params_not_strings() {
	let rune = rune_engine_source();

	assert!(rune.contains("pub enum CustomerId"));
	assert!(rune.contains("pub enum ProductId"));
	assert!(rune.contains("rune::Params::new(\"as_logical_type\", [CustomerId::HASH])"));
	assert!(rune.contains("rune::Params::new(\"as_logical_type\", [ProductId::HASH])"));
	assert!(!rune.contains("as_logical_type(self, logical_type: String)"));
	assert!(!rune.contains("normalize_rune"));
}

#[test]
pub(crate) fn adr087_cast_outputs_naked_identity() {
	let execution = execution_source();
	let binder = binder_source();
	let append_node = source_block(&execution, "fn append_node");
	let explicit_cast = source_block(&binder, "fn bind_explicit_cast");
	let try_cast = source_block(&binder, "fn bind_try_cast");

	assert!(append_node.contains("self.node_logical_types.push(LogicalTypeId::NAKED)"));
	assert!(explicit_cast.contains("builder.append_cast"));
	assert!(!explicit_cast.contains("set_node_logical_type"));
	assert!(try_cast.contains("builder.append_try_cast"));
	assert!(!try_cast.contains("set_node_logical_type"));
}

#[test]
pub(crate) fn adr088_uses_arrow_field_metadata_not_wendelon_descriptor_field() {
	let ir = ir_source();
	let wendelon = wendelon_table_source();

	assert!(ir.contains("pub const LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(ir.contains("\"nilket.toron.logical_type_id\""));
	assert!(ir.contains("field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY)"));
	assert!(!wendelon.contains("nilket.toron.logical_type_id"));
	assert!(!wendelon.contains("LogicalTypeId"));
}

#[test]
pub(crate) fn adr088_no_table_binding_parallel_logical_vector() {
	let catalog = catalog_source();
	let binding = source_block(&catalog, "pub struct TableBinding");

	assert!(!binding.contains("logical_column_types"));
	assert!(!binding.contains("Vec<LogicalTypeId>"));
	assert!(binding.contains("pub schema: SchemaRef"));
}

#[test]
pub(crate) fn adr088_resolved_column_stays_physical_only() {
	let execution = execution_source();
	let resolved = source_block(&execution, "pub struct ResolvedColumn");

	assert!(resolved.contains("pub data_type: DataType"));
	assert!(!resolved.contains("logical_type"));
}

#[test]
pub(crate) fn adr088_field_metadata_decoder_validates_parse_registry_and_physical_type() {
	let ir = ir_source();
	let decoder = source_block(&ir, "pub fn decode_column_logical_type_with_resolver");

	assert!(decoder.contains("field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY)"));
	assert!(decoder.contains("parse::<u32>()"));
	assert!(decoder.contains("LogicalTypeId::NAKED"));
	assert!(decoder.contains("resolver.spec(id)"));
	assert!(decoder.contains("spec.physical != *field.data_type()"));
	assert!(ir.contains("LogicalTypeMetadataError::InvalidDecimal"));
	assert!(ir.contains("LogicalTypeMetadataError::UnknownId"));
	assert!(ir.contains("LogicalTypeMetadataError::PhysicalMismatch"));
}

#[test]
pub(crate) fn adr088_descriptor_binding_validates_declared_logical_metadata() {
	let catalog = catalog_source();
	let from_descriptor = source_block(
		&catalog,
		"pub fn from_descriptor_with_logical_types_and_optimizer_facts",
	);

	let arrow_schema = from_descriptor
		.find("descriptor.arrow_schema()")
		.expect("descriptor schema decode");
	let validate = from_descriptor
		.find("validate_logical_column_metadata(schema.as_ref(), logical_types)?")
		.expect("logical metadata validation");
	let row_identity = from_descriptor
		.find("table_row_identity")
		.expect("row identity decode");

	assert!(arrow_schema < validate);
	assert!(validate < row_identity);
	assert!(catalog.contains("fn validate_logical_column_metadata"));
	assert!(catalog.contains("decode_column_logical_type_with_resolver(field, logical_types)"));
}

#[test]
pub(crate) fn adr088_column_binding_stamps_bound_expr_logical_sidecar() {
	let binder = binder_source();
	let bind_expr = source_block(&binder, "fn bind_expr_into_program");

	let resolve = bind_expr
		.find("resolve_expr_column(column_ref, input_schema, ctx.relation_scope)?")
		.expect("logical column resolution");
	let append = bind_expr
		.find("builder.append_column(column.column)")
		.expect("physical column append");
	let stamp = bind_expr
		.find("builder.set_node_logical_type(node, column.logical_type)")
		.expect("logical sidecar stamp");

	assert!(resolve < append);
	assert!(append < stamp);
}

#[test]
pub(crate) fn adr088_join_key_takes_identity_from_catalog_metadata() {
	let binder = binder_source();
	let scope = source_block(&binder, "fn resolve_join_column");
	let resolve = source_block(&binder, "fn resolved_logical_column_at_ordinal");
	let join_key = source_block(&binder, "fn resolved_join_key");

	assert!(scope.contains("resolve_qualified_column"));
	assert!(scope.contains("resolve_logical_column"));
	assert!(resolve.contains("decode_column_logical_type(field)"));
	assert!(join_key.contains("column.logical_type"));
	assert!(join_key.contains("declared_logical_type"));
	assert!(join_key.contains("reconcile_identity("));
}

#[test]
pub(crate) fn adr088_identity_reconciliation_is_single_shared_helper() {
	let binder = binder_source();
	let reconcile = source_block(&binder, "fn reconcile_identity");
	let ascription = source_block(&binder, "fn bind_logical_ascription");
	let join_key = source_block(&binder, "fn resolved_join_key");

	assert_eq!(binder.matches("fn reconcile_identity").count(), 1);
	assert!(reconcile.contains("existing == declared"));
	assert!(reconcile.contains("declared.is_naked()"));
	assert!(reconcile.contains("existing.is_naked()"));
	assert!(reconcile.contains("NominalTypeMismatch"));
	assert!(ascription.contains("reconcile_identity(existing, logical_type, &physical)"));
	assert!(join_key.contains("reconcile_identity("));
}

#[test]
pub(crate) fn adr088_explicit_ascription_cannot_contradict_existing_identity() {
	let binder = binder_source();
	let ascription = source_block(&binder, "fn bind_logical_ascription");
	let reconcile = source_block(&binder, "fn reconcile_identity");
	let read_existing = ascription
		.find("node_logical_type(inner)")
		.expect("existing node logical identity read");
	let reconcile_call = ascription
		.find("reconcile_identity(existing, logical_type, &physical)")
		.expect("logical identity reconciliation");
	let stamp = ascription
		.find("set_node_logical_type(inner, logical_type)")
		.expect("logical sidecar stamp");

	assert!(read_existing < reconcile_call);
	assert!(reconcile_call < stamp);
	assert!(reconcile.contains("NominalTypeMismatch"));
}

#[test]
pub(crate) fn adr088_no_column_name_heuristic_for_logical_identity() {
	let ir = ir_source();
	let binder = binder_source();
	let decoder = source_block(&ir, "pub fn decode_column_logical_type");
	let resolve = source_block(&binder, "fn resolve_logical_column");

	assert!(!decoder.contains("customer_id"));
	assert!(!decoder.contains("product_id"));
	assert!(!decoder.contains("field.name() =="));
	assert!(!resolve.contains("customer_id"));
	assert!(!resolve.contains("product_id"));
}

#[test]
pub(crate) fn adr089_logical_metadata_writer_is_centralized() {
	let ir = ir_source();
	let binder = binder_source();
	let execution = execution_source();
	let writer = source_block(&ir, "pub fn field_with_logical_type_metadata");

	assert!(writer.contains("Field::new(name, data_type, nullable)"));
	assert!(writer.contains("logical_type.is_naked()"));
	assert!(writer.contains("field.with_metadata"));
	assert!(writer.contains("LOGICAL_TYPE_ID_METADATA_KEY.to_string()"));

	let production = [ir.as_str(), execution.as_str(), binder.as_str()];
	let writer_defs = production
		.iter()
		.map(|source| {
			source
				.matches("pub fn field_with_logical_type_metadata")
				.count()
		})
		.sum::<usize>();
	assert_eq!(writer_defs, 1);

	for block in [
		source_block(&execution, "pub fn schema_for_exprs"),
		source_block(&binder, "fn schema_for_exprs_with_output_names"),
		source_block(&binder, "fn schema_for_projected_exprs_with_existing_names"),
		source_block(&binder, "fn infer_ctas_schema"),
	] {
		assert!(block.contains("field_with_logical_type_metadata"));
		assert!(!block.contains(".with_metadata"));
	}
}

#[test]
pub(crate) fn adr089_schema_for_exprs_writes_root_sidecar() {
	let execution = execution_source();
	let schema_for_exprs = source_block(&execution, "pub fn schema_for_exprs");

	assert!(execution.contains("pub fn root_logical_type(&self, root_index: usize)"));
	assert!(schema_for_exprs.contains("root_logical_type(idx)"));
	assert!(schema_for_exprs.contains("unwrap_or(LogicalTypeId::NAKED)"));
	assert!(schema_for_exprs.contains("field_with_logical_type_metadata"));
	assert!(!schema_for_exprs.contains("decode_column_logical_type"));
}

#[test]
pub(crate) fn adr089_named_projection_preserves_metadata_on_rename() {
	let binder = binder_source();
	let named_project = source_block(&binder, "fn schema_for_exprs_with_output_names");

	assert!(named_project.contains("output_names[idx]"));
	assert!(named_project.contains("root_logical_type(idx)"));
	assert!(named_project.contains("field_with_logical_type_metadata"));
	assert!(!named_project.contains("decode_column_logical_type"));
	assert!(!named_project.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
}

#[test]
pub(crate) fn adr089_project_pruning_preserves_root_identity() {
	let binder = binder_source();
	let pruned_project = source_block(&binder, "fn schema_for_projected_exprs_with_existing_names");

	assert!(pruned_project.contains("requested_outputs"));
	assert!(pruned_project.contains("root_logical_type(idx)"));
	assert!(pruned_project.contains("field_with_logical_type_metadata"));
	assert!(!pruned_project.contains("decode_column_logical_type"));
	assert!(!pruned_project.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
}

#[test]
pub(crate) fn adr089_group_keys_preserve_identity_aggregate_outputs_are_adr113_owned() {
	let binder = binder_source();
	let aggregate_schema = source_block(&binder, "fn aggregate_schema");
	let aggregate_field = source_block(&binder, "fn aggregate_field");

	assert!(aggregate_schema.contains("schema_for_exprs(group_expr)"));
	assert!(aggregate_schema.contains("fields.push(aggregate_field(call))"));
	assert!(!aggregate_schema.contains("field_with_logical_type_metadata"));
	assert!(!aggregate_schema.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(!aggregate_field.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(aggregate_field.contains("aggregate_output_logical_type(call, field.data_type())"));
}

#[test]
pub(crate) fn adr113_min_max_preserve_aggregate_identity() {
	let binder = binder_source();
	let aggregate_field = source_block(&binder, "fn aggregate_field");
	let output_identity = source_block(&binder, "fn aggregate_output_logical_type");

	assert!(aggregate_field.contains("aggregate_kernel::stable_output_field(call)"));
	assert!(aggregate_field.contains("aggregate_output_logical_type(call, field.data_type())"));
	assert!(aggregate_field.contains("field_with_logical_type_metadata"));

	assert!(output_identity.contains("call.args.root_logical_type(0)"));
	assert!(output_identity.contains("call.args.root_type(0)"));
	assert!(output_identity.contains("AggregateFunction::Min | AggregateFunction::Max"));
	assert!(output_identity.contains("input_type != output_type"));
	for naked in [
		"AggregateFunction::CountAll",
		"AggregateFunction::Count",
		"AggregateFunction::Sum",
		"AggregateFunction::Avg",
		"AggregateFunction::LinearRegress",
		"AggregateFunction::ApproxQuantile(_)",
	] {
		assert!(
			output_identity.contains(naked),
			"missing naked arm for {naked}"
		);
	}
}

#[test]
pub(crate) fn adr113_aggregate_kernel_stays_physical_only() {
	let aggregate_kernel = aggregate_kernel_source();
	let exec = exec_source();
	let pbc = pbc_source();

	for (name, source) in [
		("aggregate_kernel.rs", aggregate_kernel.as_str()),
		("exec.rs", exec.as_str()),
		("pbc.rs", pbc.as_str()),
	] {
		assert!(
			!source.contains("field_with_logical_type_metadata"),
			"{name} must not write logical field metadata"
		);
		assert!(
			!source.contains("LOGICAL_TYPE_ID_METADATA_KEY"),
			"{name} must not write logical metadata keys"
		);
	}
}

#[test]
pub(crate) fn adr089_ctas_reemits_reserved_logical_metadata_only() {
	let binder = binder_source();
	let ctas = source_block(&binder, "fn infer_ctas_schema");
	let decode = ctas
		.find("decode_column_logical_type(field)")
		.expect("CTAS decodes reserved logical metadata");
	let writer = ctas
		.find("field_with_logical_type_metadata")
		.expect("CTAS re-emits via centralized writer");

	assert!(decode < writer);
	assert!(!ctas.contains(".with_metadata"));
	assert!(!ctas.contains("metadata().clone"));
	assert!(!ctas.contains("field.metadata().clone"));
}

#[test]
pub(crate) fn adr089_ctas_if_not_exists_exact_equality_policy() {
	let coordinator = coordinator_source();
	let write = source_block(&coordinator, "async fn execute_write_plan_with_source");
	let ctas_arm = write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.expect("CTAS arm")
		.split("ExecutionPlan::Delete")
		.next()
		.expect("CTAS arm before next match arm");

	assert!(ctas_arm.contains("if *if_not_exists"));
	assert!(ctas_arm.contains("existing.schema.as_ref() == schema.as_ref()"));
	assert!(ctas_arm.contains("CatalogError::SchemaMismatch"));
}

#[test]
pub(crate) fn adr089_no_lineage_or_name_heuristic_inference() {
	let binder = binder_source();
	let execution = execution_source();

	for block in [
		source_block(&execution, "pub fn schema_for_exprs"),
		source_block(&binder, "fn schema_for_exprs_with_output_names"),
		source_block(&binder, "fn schema_for_projected_exprs_with_existing_names"),
		source_block(&binder, "fn infer_ctas_schema"),
	] {
		assert!(!block.contains("customer_id"));
		assert!(!block.contains("product_id"));
		assert!(!block.contains("logical_type_spec"));
		assert!(!block.contains("root_columns"));
		assert!(!block.contains("column_node"));
		assert!(!block.contains("resolve_table"));
	}
}

#[test]
pub(crate) fn adr090_logical_type_authority_is_wendelon_metadata_not_toron_query_path() {
	let compute = wendelon_compute_source();
	let raft = wendelon_raft_storage_source();
	let storage = storage_source();

	assert!(compute.contains("SchemaObjectKind"));
	assert!(compute.contains("LogicalType"));
	assert!(compute.contains("pub struct LogicalTypeDefinition"));
	assert!(raft.contains("pub logical_types: LogicalTypeCatalog"));
	assert!(raft.contains("apply_logical_type_schema_update"));
	assert!(storage.contains("submit_logical_type_definition_create"));
	assert!(storage.contains("get_logical_type_definition"));
	assert!(catalog_source().contains("list_logical_type_definitions"));
	assert!(!storage.contains("SELECT * FROM"));
	assert!(!storage.contains("information_schema.logical_types"));
}

#[test]
pub(crate) fn adr090_logical_type_id_allocation_is_wendelon_owned_and_durable() {
	let compute = wendelon_compute_source();

	assert!(compute.contains("LOGICAL_TYPE_RESERVED_STATIC_MAX_ID"));
	assert!(compute.contains("fn next_id_for_tenant"));
	assert!(compute.contains("logical_type_id: self.next_id_for_tenant(tenant_id)?"));
	assert!(compute.contains("definitions_for_tenant"));
	assert!(compute.contains("Serialize, Deserialize"));
}

#[test]
pub(crate) fn adr090_decode_resolves_against_synced_registry_not_static_hardcode() {
	let ir = ir_source();
	let catalog = catalog_source();

	assert!(ir.contains("pub struct LogicalTypeRegistry"));
	assert!(ir.contains("pub struct LogicalTypeResolver"));
	assert!(ir.contains("decode_column_logical_type_with_resolver"));
	assert!(catalog.contains("TableBinding::from_descriptor_with_logical_types"));
	assert!(catalog.contains("register_logical_type_for_tenant"));
	assert!(catalog.contains("register_logical_types_for_tenant"));
	assert!(catalog.contains("LogicalTypeResolver::with_registry"));
}

#[test]
pub(crate) fn adr090_no_process_global_dynamic_logical_type_registry() {
	let ir = ir_source();
	let catalog = catalog_source();

	assert!(!ir.contains("static LOGICAL_TYPE_REGISTRY"));
	assert!(!ir.contains("lazy_static!"));
	assert!(!ir.contains("OnceLock<LogicalTypeRegistry"));
	assert!(!ir.contains("Mutex<LogicalTypeRegistry"));
	assert!(!catalog.contains("static LOGICAL_TYPE_REGISTRY"));
}

#[test]
pub(crate) fn adr090_create_logical_type_keyword_not_create_domain() {
	let planner = sql_planner_source();

	assert!(planner.contains("CREATE LOGICAL TYPE"));
	assert!(planner.contains("try_lower_create_logical_type"));
	assert!(!planner.contains("CREATE DOMAIN"));
}

#[test]
pub(crate) fn adr090_static_pilot_seed_is_reserved_range_rune_test_bridge_only() {
	let ir = ir_source();
	let rune = rune_engine_source();

	assert!(ir.contains("pub const CUSTOMER_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(1);"));
	assert!(ir.contains("pub const PRODUCT_ID_LOGICAL_TYPE: LogicalTypeId = LogicalTypeId(2);"));
	assert!(ir.contains("pub const DURABLE_LOGICAL_TYPE_MIN_ID"));
	assert!(rune.contains("CUSTOMER_ID_LOGICAL_TYPE"));
	assert!(rune.contains("PRODUCT_ID_LOGICAL_TYPE"));
}

#[test]
pub(crate) fn adr090_no_dropalter_or_pkfk_derivation_after_refinement_persistence() {
	let compute = wendelon_compute_source();
	let raft = wendelon_raft_storage_source();
	let planner = sql_planner_source();

	assert!(raft.contains("logical type ALTER/DROP is not supported"));
	assert!(!planner.contains("FOREIGN KEY") || !planner.contains("LogicalTypePhysical"));
	assert!(compute.contains("LogicalTypeRefinementPredicate"));
	assert!(compute.contains("pub refinement: Option<LogicalTypeRefinementPredicate>"));
	assert!(!compute.contains("BoundExprProgram"));
	assert!(!compute.contains("ScalarValue"));
}

#[test]
pub(crate) fn adr091_create_table_carries_unresolved_logical_type_names_to_bind() {
	let ir = ir_source();
	let logical_plan = ir
		.split("pub enum LogicalPlan")
		.nth(1)
		.and_then(|rest| rest.split("pub fn project_plan").next())
		.expect("LogicalPlan enum");
	let create_table = source_block(logical_plan, "CreateTable {");

	assert!(ir.contains("pub struct CreateTableSchema"));
	assert!(ir.contains("pub struct CreateTableColumn"));
	assert!(ir.contains("pub enum CreateTableColumnType"));
	assert!(ir.contains("LogicalName(String)"));
	assert!(create_table.contains("schema: CreateTableSchema,"));
	assert!(!create_table.contains("schema: SchemaRef,"));
}

#[test]
pub(crate) fn adr091_execution_create_table_stays_resolved_schema() {
	let execution = execution_source();
	let execution_plan = execution
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan enum");
	let create_table = source_block(execution_plan, "CreateTable {");

	assert!(create_table.contains("schema: SchemaRef,"));
	assert!(!create_table.contains("CreateTableSchema"));
	assert!(!execution.contains("CreateTableColumnType"));
}

#[test]
pub(crate) fn adr091_sql_planner_records_custom_type_names_without_catalog_resolution() {
	let planner = sql_planner_source();
	let lower_type = source_block(&planner, "fn lower_create_table_data_type");
	let lower_logical = source_block(&planner, "fn lower_sql_logical_type_name");
	let validate_ident = source_block(&planner, "fn validate_sql_logical_type_ident");
	let validate_name = source_block(&planner, "fn validate_sql_logical_type_name");

	assert!(lower_type.contains("SqlDataType::Custom(name, modifiers)"));
	assert!(lower_type.contains("CreateTableColumnType::LogicalName"));
	assert!(lower_logical.contains("validate_sql_logical_type_ident"));
	assert!(validate_ident.contains("ident.quote_style.is_some()"));
	assert!(validate_name.contains("eq_ignore_ascii_case(\"naked\")"));
	assert!(validate_name.contains("Ok(name.to_string())"));
	assert!(!lower_type.contains("resolve_durable_logical_type_by_name"));
	assert!(!lower_type.contains("resolve_logical_type_by_name"));
}

#[test]
pub(crate) fn adr091_ddl_logical_type_resolution_is_registry_only_not_seed_first() {
	let ir = ir_source();
	let catalog = catalog_source();
	let binder = binder_source();
	let bind_create = source_block(&binder, "async fn bind_create_table");

	assert!(ir.contains("pub fn durable_only(registry: &'a LogicalTypeRegistry) -> Self"));
	assert!(ir.contains("include_seed: false"));
	assert!(catalog.contains("resolve_durable_logical_type_by_name"));
	assert!(catalog.contains("LogicalTypeResolver::durable_only(registry).spec_by_name(name)"));
	assert!(bind_create.contains("resolve_durable_logical_type_by_name(name)"));
	assert!(!bind_create.contains("resolve_logical_type_by_name"));
	assert!(!bind_create.contains("logical_type_spec_by_name"));
}

#[test]
pub(crate) fn adr091_wendelon_lookup_refreshes_logical_type_list_before_unknown() {
	let catalog = catalog_source();
	let impl_body = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	let lookup = source_block(impl_body, "async fn resolve_durable_logical_type_by_name");
	let local = lookup
		.find("resolve_durable_logical_type_by_name(self.tenant_id, name)")
		.expect("local durable lookup");
	let list = lookup
		.find("list_logical_type_definitions")
		.expect("logical type list refresh");
	let not_found = lookup
		.rfind("CatalogError::LogicalTypeNotFound")
		.expect("not found after refresh");

	assert!(local < list);
	assert!(list < not_found);
	assert!(lookup.contains("register_logical_types_for_tenant"));
}

#[test]
pub(crate) fn adr091_ddl_logical_column_uses_089_metadata_writer() {
	let binder = binder_source();
	let bind_create = source_block(&binder, "async fn bind_create_table");

	assert!(bind_create.contains("CreateTableColumnType::Physical"));
	assert!(bind_create.contains("Field::new"));
	assert!(bind_create.contains("CreateTableColumnType::LogicalName"));
	assert!(bind_create.contains("field_with_logical_type_metadata"));
	assert!(!bind_create.contains(".with_metadata"));
}

#[test]
pub(crate) fn adr091_catalog_create_rebinds_logical_descriptors_with_resolver() {
	let catalog = catalog_source();
	let mock_impl = catalog
		.split("impl Catalog for MockCatalog")
		.nth(1)
		.and_then(|rest| rest.split("impl WendelonDescriptorRegistry").next())
		.expect("MockCatalog impl");
	let mock_create = source_block(mock_impl, "async fn create_table_with_outcome");
	let wendelon_impl = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog impl");
	let wendelon_create = source_block(wendelon_impl, "async fn create_table_with_outcome");

	assert!(mock_create.contains("inner\n\t\t\t.logical_types"));
	assert!(wendelon_create.contains("logical_type_registry"));
	for body in [mock_create, wendelon_create] {
		assert!(body.contains("TableBinding::from_descriptor_with_logical_types"));
		assert!(body.contains("LogicalTypeResolver::with_registry"));
		assert!(!body.contains("TableBinding::from_descriptor(legacy_table_id, &descriptor)"));
	}
}

#[test]
pub(crate) fn adr091_undefined_logical_type_fails_before_descriptor_submit() {
	let binder = binder_source();
	let bind_create = source_block(&binder, "async fn bind_create_table");
	let resolve = bind_create
		.find("resolve_durable_logical_type_by_name(name)")
		.expect("logical name resolution");
	let execution = bind_create
		.find("ExecutionPlan::CreateTable")
		.expect("resolved execution plan");
	let catalog = catalog_source();

	assert!(resolve < execution);
	assert!(error_source().contains("LogicalTypeNotFound"));
	assert!(catalog.contains("CatalogError::LogicalTypeNotFound"));
}

#[test]
pub(crate) fn adr091_typed_ctas_authoring_is_still_deferred_after_adr092() {
	let planner = sql_planner_source();
	let ctas = source_block(&planner, "fn lower_create_table_as_select");

	assert!(ctas.contains("CTAS does not support explicit column lists"));
	assert!(!ctas.contains("CreateTableSchema"));
	assert!(!ctas.contains("LogicalName"));
}

#[test]
pub(crate) fn adr092_sql_logical_cast_is_transient_logical_ir_only() {
	let ir = ir_source();
	let expr_enum = ir
		.split("pub enum Expr")
		.nth(1)
		.and_then(|rest| rest.split("impl Expr").next())
		.expect("Expr enum");
	let execution = execution_source();

	assert!(expr_enum.contains("SqlLogicalCast {"));
	assert!(expr_enum.contains("logical_name: String"));
	assert!(expr_enum.contains("try_cast: bool"));
	assert!(expr_enum.contains("LogicalAscription {"));
	assert!(!expr_enum.contains("coerce"));
	assert!(!execution.contains("SqlLogicalCast"));
}

#[test]
pub(crate) fn adr092_planner_records_logical_cast_names_without_resolution() {
	let planner = sql_planner_source();
	let lower_cast = source_block(&planner, "fn lower_cast_target");
	let lower_logical = source_block(&planner, "fn lower_cast_logical_type_name");
	let lower_shared = source_block(&planner, "fn lower_sql_logical_type_name");
	let validate_ident = source_block(&planner, "fn validate_sql_logical_type_ident");
	let validate_name = source_block(&planner, "fn validate_sql_logical_type_name");
	let lower_expr = source_block(&planner, "fn lower_expr");

	assert!(lower_cast.contains("SqlDataType::Custom(name, modifiers)"));
	assert!(lower_cast.contains("LoweredCastTarget::LogicalName"));
	assert!(lower_logical.contains("lower_sql_logical_type_name(\"CAST\""));
	assert!(lower_shared.contains("validate_sql_logical_type_ident"));
	assert!(validate_ident.contains("ident.quote_style.is_some()"));
	assert!(validate_name.contains("eq_ignore_ascii_case(\"naked\")"));
	assert!(lower_expr.contains("Expr::SqlLogicalCast"));
	assert!(lower_expr.contains("try_cast: true"));
	for block in [
		lower_cast,
		lower_logical,
		lower_shared,
		validate_ident,
		validate_name,
		lower_expr,
	] {
		assert!(!block.contains("resolve_durable_logical_type_by_name"));
		assert!(!block.contains("logical_type_spec_by_name"));
	}
}

#[test]
pub(crate) fn adr092_binder_resolves_sql_logical_casts_registry_only_before_sync_bind() {
	let binder = binder_source();
	let resolve = source_block(&binder, "async fn resolve_logical_casts");
	let collect = source_block(&binder, "fn collect_logical_cast_names");
	let bind_program = source_block(&binder, "async fn bind_expr_program_with_scope");

	assert!(resolve.contains("collect_logical_cast_names(expr"));
	assert!(resolve.contains("resolve_durable_logical_type_by_name(&name)"));
	assert!(resolve.contains("HashMap<String, LogicalTypeSpec>"));
	assert!(bind_program.contains("ExprBindContext"));
	assert!(bind_program.contains("logical_type_specs: &logical_casts"));
	assert!(bind_program.contains("logical_type_registry: &logical_type_registry"));
	assert!(collect.contains("Expr::SqlLogicalCast"));
	assert!(collect.contains("names.insert(logical_name.clone())"));
	assert!(collect.contains("Expr::SqlConvert"));
	assert!(collect.contains("names.insert(target_logical_name.clone())"));
	assert!(!resolve.contains("logical_type_spec_by_name"));
	assert!(!resolve.contains("seed_logical_type_spec_by_name"));
}

#[test]
pub(crate) fn adr092_logical_cast_reconciles_identity_before_physical_cast() {
	let binder = binder_source();
	let bind_cast = source_block(&binder, "fn bind_sql_logical_cast");
	let reconcile = bind_cast
		.find("reconcile_identity(existing, spec.id, &source)")
		.expect("logical identity reconciliation");
	let non_null_branch = &bind_cast[reconcile..];
	let append_cast = non_null_branch
		.find("builder.append_cast")
		.expect("physical cast append");
	let stamp = non_null_branch
		.find("builder.set_node_logical_type(output, logical_type)")
		.expect("logical type stamp");

	assert!(append_cast < stamp);
	assert!(bind_cast.contains("source == spec.physical"));
	assert!(bind_cast.contains("is_supported_explicit_numeric_cast(&source, &spec.physical)"));
	assert!(bind_cast.contains("unsupported CAST: {source:?} to logical type"));
	assert!(!bind_cast.contains("validate_logical_ascription_physical"));
	assert!(!bind_cast.contains("logical_type_spec("));
}

#[test]
pub(crate) fn adr092_sql_logical_cast_does_not_add_execution_kernel_or_pbc_opcode() {
	let ir = ir_source();
	let exec = exec_source();
	let pbc = pbc_source();
	let pbc_helper = source_block(&ir, "pub fn is_pbc_pushdown_eligible");

	assert!(pbc_helper.contains("Self::SqlLogicalCast { .. }"));
	assert!(pbc_helper.contains("=> false"));
	assert!(exec.contains("unresolved SQL logical CAST reached execution"));
	assert!(!exec.contains("fn evaluate_sql_logical_cast"));
	assert!(!pbc.contains("SqlLogicalCast"));
	assert!(!pbc.contains("OP_LOGICAL_CAST"));
}

#[test]
pub(crate) fn adr167_cast_path_contains_no_unit_transform() {
	let binder = binder_source();
	let bind_cast = source_block(&binder, "fn bind_sql_logical_cast");

	assert!(bind_cast.contains("source == spec.physical"));
	assert!(bind_cast.contains("is_supported_explicit_numeric_cast"));
	assert!(!bind_cast.contains("scale_to_base"));
	assert!(!bind_cast.contains("unit_scale_to_f64"));
	assert!(!bind_cast.contains("bind_sql_convert"));
	assert!(!bind_cast.contains("SqlConvert"));
	assert!(!bind_cast.contains("dimension mismatch"));
}

#[test]
pub(crate) fn adr167_convert_is_transient_ir_and_bind_desugared() {
	let ir = ir_source();
	let execution = execution_source();
	let binder = binder_source();
	let expr_enum = ir
		.split("pub enum Expr")
		.nth(1)
		.and_then(|rest| rest.split("impl Expr").next())
		.expect("Expr enum");
	let bound_node = source_block(&execution, "pub enum BoundExprNode");
	let execution_plan = source_block(&execution, "pub enum ExecutionPlan");
	let bind_expr = source_block(&binder, "fn bind_expr_into_program");
	let bind_convert = source_block(&binder, "fn bind_sql_convert");

	assert!(expr_enum.contains("SqlConvert {"));
	assert!(expr_enum.contains("target_logical_name: String"));
	assert!(!bound_node.contains("SqlConvert"));
	assert!(!execution_plan.contains("SqlConvert"));
	assert!(bind_expr.contains("Expr::SqlConvert"));
	assert!(bind_expr.contains("bind_sql_convert"));
	assert!(bind_convert.contains("unit_scale_to_f64(&source_unit.scale_to_base)"));
	assert!(bind_convert.contains("unit_scale_to_f64(&target_unit.scale_to_base)"));
	assert!(bind_convert.contains("builder.append_literal(ScalarValue::Float64"));
	assert!(bind_convert.contains("builder.append_binary(BoundBinaryOp::Mul"));
	assert!(bind_convert.contains("builder.set_node_logical_type(output, target_spec.id)"));
}

#[test]
pub(crate) fn adr167_no_executor_kernel_or_pbc_opcode() {
	let ir = ir_source();
	let exec = exec_source();
	let pbc = pbc_source();
	let pbc_helper = source_block(&ir, "pub fn is_pbc_pushdown_eligible");

	assert!(pbc_helper.contains("Self::SqlConvert { .. }"));
	assert!(pbc_helper.contains("=> false"));
	assert!(exec.contains("unresolved SQL CONVERT reached execution"));
	assert!(!exec.contains("fn evaluate_sql_convert"));
	assert!(!exec.contains("evaluate_convert"));
	assert!(!pbc.contains("SqlConvert"));
	assert!(!pbc.contains("OP_CONVERT"));
}

#[test]
pub(crate) fn adr167_units_are_durable_logical_type_metadata_not_arrow_sidecar() {
	let ir = ir_source();
	let execution = execution_source();
	let storage = storage_source();
	let wendelon = wendelon_compute_source();
	let spec = source_block(&ir, "pub struct LogicalTypeSpec");
	let create_plan = source_block(&execution, "pub enum ExecutionPlan");
	let create_request = source_block(&storage, "pub struct CreateLogicalTypeRequest");
	let field_writer = source_block(&ir, "pub fn field_with_logical_type_metadata");

	assert!(spec.contains("pub unit: Option<LogicalTypeUnit>"));
	assert!(create_plan.contains("unit: Option<wendelon_client::LogicalTypeUnit>"));
	assert!(create_request.contains("pub unit: Option<wendelon_client::LogicalTypeUnit>"));
	assert!(wendelon.contains("pub struct LogicalTypeUnit"));
	assert!(wendelon.contains("pub unit: Option<LogicalTypeUnit>"));
	assert!(field_writer.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(!field_writer.contains("LogicalTypeUnit"));
	assert!(!field_writer.contains("scale_to_base"));
	assert!(!field_writer.contains("dimension"));
}

#[test]
pub(crate) fn adr167_unit_sql_resolution_is_durable_only_not_seed_first() {
	let binder = binder_source();
	let planner = sql_planner_source();
	let resolve = source_block(&binder, "async fn resolve_logical_casts");
	let collect = source_block(&binder, "fn collect_logical_cast_names");
	let bind_convert = source_block(&binder, "fn bind_sql_convert");
	let lower_convert = source_block(&planner, "fn lower_convert(");
	let lower_convert_ast = source_block(&planner, "fn lower_convert_ast");
	let lower_target = source_block(&planner, "fn lower_convert_target_logical_name");

	assert!(resolve.contains("resolve_durable_logical_type_by_name(&name)"));
	assert!(!resolve.contains("logical_type_spec_by_name"));
	assert!(!resolve.contains("seed_logical_type_spec_by_name"));
	assert!(collect.contains("Expr::SqlConvert"));
	assert!(collect.contains("names.insert(target_logical_name.clone())"));
	assert!(bind_convert.contains("logical_type_specs"));
	assert!(bind_convert.contains("LogicalTypeResolver::with_registry(ctx.logical_type_registry)"));
	assert!(!bind_convert.contains("seed_logical_type_spec"));
	assert!(lower_convert.contains("Expr::SqlConvert"));
	assert!(lower_convert_ast.contains("Expr::SqlConvert"));
	assert!(lower_target.contains("validate_sql_logical_type_name"));
	assert!(!lower_convert.contains("resolve_durable_logical_type_by_name"));
	assert!(!lower_convert_ast.contains("resolve_durable_logical_type_by_name"));
	assert!(!lower_target.contains("resolve_durable_logical_type_by_name"));
}

#[test]
pub(crate) fn adr167_dimensioned_arithmetic_rejects_implicit_unit_coercion() {
	let binder = binder_source();
	let arithmetic = source_block(&binder, "fn bind_arithmetic");
	let dimensioned = source_block(&binder, "fn bind_dimensioned_arithmetic");
	let comparison = source_block(&binder, "fn bind_dimensioned_comparison");
	let compatible = source_block(&binder, "fn require_compatible_units");
	let dimensioned_call = arithmetic
		.find("bind_dimensioned_arithmetic")
		.expect("dimensioned arithmetic gate");
	let numeric_fallback = arithmetic
		.find("arithmetic_output_type(&left_type, &right_type)")
		.expect("ordinary numeric fallback");

	assert!(dimensioned_call < numeric_fallback);
	assert!(dimensioned.contains("BoundBinaryOp::Add | BoundBinaryOp::Sub"));
	assert!(dimensioned.contains("unit arithmetic requires both operands"));
	assert!(
		dimensioned.contains("unit multiplication that creates derived dimensions is deferred")
	);
	assert!(dimensioned.contains("naked numeric divided by a unit is deferred"));
	assert!(comparison.contains("unit comparison requires both operands"));
	assert!(compatible.contains("require explicit CONVERT"));
	assert!(compatible.contains("dimension mismatch"));
}

#[test]
pub(crate) fn adr167_linear_float64_only_no_affine_offset() {
	let binder = binder_source();
	let planner = sql_planner_source();
	let wendelon = wendelon_compute_source();
	let bind_convert = source_block(&binder, "fn bind_sql_convert");
	let unit_scale = source_block(&binder, "fn unit_scale_to_f64");
	let parse_clauses = source_block(&planner, "fn parse_create_logical_type_clauses");
	let scale_parser = source_block(&planner, "fn parse_positive_decimal_scale");
	let unit_validate = source_block(&wendelon, "impl LogicalTypeUnit");

	assert!(parse_clauses.contains("LogicalTypePhysical::Float64"));
	assert!(unit_validate.contains("LogicalTypePhysical::Float64"));
	assert!(scale_parser.contains("numerator == 0"));
	assert!(unit_scale.contains("scale.numerator as f64 / scale.denominator as f64"));
	assert!(bind_convert.contains("source_type != DataType::Float64"));
	assert!(bind_convert.contains("target_spec.physical != DataType::Float64"));
	assert!(bind_convert.contains("builder.append_binary(BoundBinaryOp::Mul"));
	assert!(!bind_convert.contains("BoundBinaryOp::Add"));
	assert!(!bind_convert.contains("BoundBinaryOp::Sub"));
	for source in [bind_convert, parse_clauses, unit_validate] {
		assert!(!source.to_ascii_lowercase().contains("offset"));
		assert!(!source.to_ascii_lowercase().contains("affine"));
	}
}

#[test]
pub(crate) fn adr167_frontends_share_convert_bind_path() {
	let planner = sql_planner_source();
	let rune = rune_engine_source();
	let binder = binder_source();
	let lower_sql = source_block(&planner, "fn lower_convert(");
	let lower_sql_ast = source_block(&planner, "fn lower_convert_ast");
	let rune_convert = source_block(&rune, "pub fn convert");
	let rune_module = source_block(&rune, "pub fn create_toron_module");
	let bind_expr = source_block(&binder, "fn bind_expr_into_program");

	assert!(lower_sql.contains("Expr::SqlConvert"));
	assert!(lower_sql_ast.contains("Expr::SqlConvert"));
	assert!(rune_convert.contains("Expr::SqlConvert"));
	assert!(rune_module.contains("m.function([\"convert\"], convert).build()?"));
	assert!(rune_module.contains("m.associated_function(\"convert\", RuneExpr::convert)?"));
	assert!(bind_expr.contains("Expr::SqlConvert"));
	assert!(bind_expr.contains("bind_sql_convert"));
}

#[test]
pub(crate) fn adr167_wendelon_unit_names_are_brand_clean_and_unsuffixed() {
	let wendelon = wendelon_compute_source();
	let client = wendelon_client_lib_source();

	assert!(wendelon.contains("pub struct UnitScale"));
	assert!(wendelon.contains("pub struct LogicalTypeUnit"));
	assert!(client.contains("LogicalTypeUnit"));
	assert!(client.contains("UnitScale"));
	for bad_name in [
		"UnitScaleV1",
		"LogicalTypeUnitV1",
		"DimensionedQuantityV1",
		"UnitDescriptorV1",
		"UnitPolicyV1",
	] {
		assert!(!wendelon.contains(bad_name));
		assert!(!client.contains(bad_name));
	}
}

#[test]
pub(crate) fn adr093_binder_collects_both_cast_and_try_cast_logical_names() {
	let binder = binder_source();
	let collect = source_block(&binder, "fn collect_logical_cast_names");

	assert!(collect.contains("Expr::SqlLogicalCast"));
	assert!(collect.contains("try_cast: _"));
	assert!(collect.contains("names.insert(logical_name.clone())"));
	assert!(collect.contains("collect_logical_cast_names(expr, names)"));
	assert!(!collect.contains("TRY_CAST/SAFE_CAST to logical type is not supported"));
}

#[test]
pub(crate) fn adr093_logical_try_cast_reconciles_identity_before_try_cast() {
	let binder = binder_source();
	let bind_cast = source_block(&binder, "fn bind_sql_logical_cast");
	let reconcile = bind_cast
		.find("reconcile_identity(existing, spec.id, &source)")
		.expect("logical identity reconciliation");
	let try_branch = bind_cast
		.find("let output = if try_cast")
		.expect("logical TRY_CAST branch");
	let try_branch_body = &bind_cast[try_branch..];
	let append_try = try_branch_body
		.find("builder.append_try_cast")
		.expect("TRY_CAST append");
	let stamp = try_branch_body
		.find("builder.set_node_logical_type(output, logical_type)")
		.expect("logical type stamp");

	assert!(reconcile < try_branch);
	assert!(append_try < stamp);
}

#[test]
pub(crate) fn adr093_logical_try_cast_categorically_nullable_no_same_physical_elide() {
	let binder = binder_source();
	let execution = execution_source();
	let bind_cast = source_block(&binder, "fn bind_sql_logical_cast");
	let node_nullable = source_block(&execution, "pub fn node_nullable");
	let try_branch = bind_cast
		.find("let output = if try_cast")
		.expect("logical TRY_CAST branch");
	let append_try = bind_cast
		.find("builder.append_try_cast")
		.expect("TRY_CAST append");
	let strict_same = bind_cast
		.find("} else if source == spec.physical")
		.expect("strict same-physical elision branch");

	assert!(try_branch < append_try);
	assert!(append_try < strict_same);
	assert!(bind_cast.contains(
		"source == spec.physical || is_supported_explicit_numeric_cast(&source, &spec.physical)"
	));
	assert!(
		bind_cast.contains(
			"builder.append_try_cast(inner, spec.physical.clone(), spec.physical.clone())"
		)
	);
	assert!(node_nullable.contains("BoundExprNode::TryCast { .. } => Some(true)"));
}

#[test]
pub(crate) fn adr093_logical_try_cast_uses_existing_try_cast_node_no_executor_or_pbc() {
	let binder = binder_source();
	let execution = execution_source();
	let exec = exec_source();
	let pbc = pbc_source();
	let bind_cast = source_block(&binder, "fn bind_sql_logical_cast");

	assert!(bind_cast.contains("builder.append_try_cast"));
	assert!(execution.contains("BoundExprNode::TryCast"));
	assert!(exec.contains("BoundExprNode::TryCast"));
	assert!(!execution.contains("SqlLogicalTryCast"));
	assert!(!exec.contains("fn evaluate_sql_logical_try_cast"));
	assert!(!exec.contains("fn evaluate_logical_try_cast"));
	assert!(!pbc.contains("SqlLogicalTryCast"));
	assert!(!pbc.contains("OP_LOGICAL_TRY_CAST"));
}

#[test]
pub(crate) fn adr093_physical_bind_try_cast_remains_identity_free() {
	let binder = binder_source();
	let try_cast = source_block(&binder, "fn bind_try_cast");

	assert!(try_cast.contains("is_supported_explicit_numeric_cast(&source, target)"));
	assert!(try_cast.contains("builder.append_try_cast"));
	assert!(!try_cast.contains("reconcile_identity"));
	assert!(!try_cast.contains("set_node_logical_type"));
	assert!(!try_cast.contains("logical_casts"));
}

#[test]
pub(crate) fn adr093_ctas_try_cast_nullability_flows_through_089_writer() {
	let binder = binder_source();
	let execution = execution_source();
	let schema_for_exprs = source_block(&execution, "pub fn schema_for_exprs");
	let named_project = source_block(&binder, "fn schema_for_exprs_with_output_names");
	let ctas = source_block(&binder, "fn infer_ctas_schema");

	assert!(schema_for_exprs.contains("root_nullable(idx)"));
	assert!(named_project.contains("root_nullable(idx)"));
	assert!(ctas.contains("field.is_nullable()"));
	for block in [schema_for_exprs, named_project, ctas] {
		assert!(block.contains("field_with_logical_type_metadata"));
	}
}

#[test]
pub(crate) fn adr099_information_schema_logical_types_intercepts_before_catalog_resolve_table() {
	let binder = binder_source();
	let scan_named = binder
		.find("LogicalPlan::ScanNamed { table, projection } =>")
		.expect("ScanNamed bind arm");
	let scan_tail = &binder[scan_named..];
	let info_schema = scan_tail
		.find("is_information_schema_namespace(table)")
		.expect("information_schema interception");
	let resolve = scan_tail
		.find("self.catalog.resolve_table(table)")
		.expect("catalog resolve table");

	assert!(info_schema < resolve);
	assert!(scan_tail.contains("bind_information_schema_relation"));
}

#[test]
pub(crate) fn adr099_system_table_is_coordinator_local_scalar_rows() {
	let execution = execution_source();
	let exec = exec_source();
	let system_kind = source_block(&execution, "pub enum SystemTableKind");
	let execution_plan = source_block(&execution, "pub enum ExecutionPlan");

	assert!(system_kind.contains("InformationSchemaLogicalTypes"));
	assert!(execution_plan.contains("SystemTable {"));
	assert!(execution_plan.contains("rows: Vec<Vec<ScalarValue>>"));
	assert!(exec.contains("ExecutionPlan::SystemTable { schema, rows, .. }"));
	assert!(exec.contains("materialize_scalar_rows(schema, rows)"));
	assert!(!execution.contains("LogicalPlan::SystemTable"));
}

#[test]
pub(crate) fn adr099_logical_types_rows_from_refreshed_durable_registry_not_seed() {
	let binder = binder_source();
	let catalog = catalog_source();
	let ir = ir_source();
	let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");
	let row_builder = source_block(&binder, "fn logical_type_spec_row");

	assert!(bind_relation.contains("refreshed_logical_type_registry"));
	assert!(bind_relation.contains("specs_sorted()"));
	assert!(row_builder.contains("spec.refinement"));
	assert!(!bind_relation.contains("logical_type_spec("));
	assert!(catalog.contains("async fn refreshed_logical_type_registry"));
	assert!(catalog.contains("list_logical_type_definitions"));
	assert!(catalog.contains("register_logical_types_for_tenant"));
	assert!(ir.contains("pub fn specs_sorted(&self) -> Vec<LogicalTypeSpec>"));
}

#[test]
pub(crate) fn adr099_information_schema_namespace_is_reserved_read_only() {
	let binder = binder_source();
	let reject = source_block(&binder, "fn reject_information_schema_target");

	assert!(reject.contains("is_information_schema_namespace(table)"));
	assert!(reject.contains("information_schema is read-only"));
	for method in [
		"async fn bind_create_table",
		"async fn bind_create_table_as_select",
		"async fn bind_delete",
		"async fn bind_insert",
		"async fn bind_insert_select",
		"async fn bind_update",
	] {
		let block = source_block(&binder, method);
		assert!(block.contains("reject_information_schema_target"));
	}
	assert!(binder.contains(
		"LogicalPlan::DropTable { name, if_exists } => {\n\t\t\t\t\treject_information_schema_target(name)?;"
	));
}

#[test]
pub(crate) fn adr099_system_table_walkers_handle_new_variant() {
	let execution = execution_source();
	let binder = binder_source();
	let exec = exec_source();
	let fragment = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragment source");
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let server = std::fs::read_to_string(manifest_dir().join("src/server/mod.rs"))
		.expect("read server source");

	assert!(source_block(&execution, "pub fn schema").contains("Self::SystemTable { schema, .. }"));
	assert!(source_block(&binder, "fn terminal_exchange").contains("ExecutionPlan::SystemTable"));
	assert!(
		source_block(&binder, "fn optimize_for_required").contains("ExecutionPlan::SystemTable")
	);
	assert!(exec.contains("ExecutionPlan::SystemTable { schema, rows, .. }"));
	assert!(fragment.matches("ExecutionPlan::SystemTable").count() >= 6);
	assert!(scheduler.contains("ExecutionPlan::SystemTable"));
	assert!(server.contains("ExecutionPlan::SystemTable { .. } => \"system_table\""));
}

#[test]
pub(crate) fn adr099_no_wendelon_pbc_wire_or_storage_surface() {
	let binder = binder_source();
	let storage = storage_source();
	let pbc = pbc_source();

	assert!(binder.contains("information_schema.logical_types"));
	for source in [storage, pbc] {
		assert!(!source.contains("information_schema.logical_types"));
		assert!(!source.contains("InformationSchemaLogicalTypes"));
	}
}

#[test]
pub(crate) fn adr102_system_table_kind_has_finite_introspection_family() {
	let execution = execution_source();
	let system_kind = source_block(&execution, "pub enum SystemTableKind");

	assert_eq!(enum_variant_count(&system_kind), 5);
	assert!(system_kind.contains("InformationSchemaLogicalTypes"));
	assert!(system_kind.contains("InformationSchemaTables"));
	assert!(system_kind.contains("InformationSchemaColumns"));
	assert!(system_kind.contains("InformationSchemaSchemata"));
	assert!(system_kind.contains("OptimizerExplain"));
	assert!(!system_kind.contains("VirtualTable"));
	assert!(!system_kind.contains("SystemTableRegistry"));
}

#[test]
pub(crate) fn adr102_information_schema_dispatch_is_explicit() {
	let binder = binder_source();
	let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");

	assert!(
		binder.contains("const INFORMATION_SCHEMA_LOGICAL_TYPES_TABLE: &str = \"logical_types\"")
	);
	assert!(binder.contains("const INFORMATION_SCHEMA_TABLES_TABLE: &str = \"tables\""));
	assert!(binder.contains("const INFORMATION_SCHEMA_COLUMNS_TABLE: &str = \"columns\""));
	assert!(binder.contains("const INFORMATION_SCHEMA_SCHEMATA_TABLE: &str = \"schemata\""));
	assert!(bind_relation.contains("eq_ignore_ascii_case(INFORMATION_SCHEMA_LOGICAL_TYPES_TABLE)"));
	assert!(bind_relation.contains("eq_ignore_ascii_case(INFORMATION_SCHEMA_TABLES_TABLE)"));
	assert!(bind_relation.contains("eq_ignore_ascii_case(INFORMATION_SCHEMA_COLUMNS_TABLE)"));
	assert!(bind_relation.contains("eq_ignore_ascii_case(INFORMATION_SCHEMA_SCHEMATA_TABLE)"));
	assert!(bind_relation.contains("unknown information_schema relation"));
	assert!(!bind_relation.contains("HashMap"));
	assert!(!bind_relation.contains("SystemTableRegistry"));
}

#[test]
pub(crate) fn adr102_table_descriptor_refresh_is_authoritative() {
	let catalog = catalog_source();
	let client = wendelon_client_core_source();
	let wendelon_table = wendelon_table_source();
	let control = wendelon_control_source();
	let wendelon_catalog = source_block(&catalog, "impl Catalog for WendelonCatalog");
	let refresh = source_block(
		&wendelon_catalog,
		"async fn refreshed_table_schema_descriptors",
	);

	assert!(catalog.contains("async fn refreshed_table_schema_descriptors"));
	assert!(catalog.contains("async fn list_table_schema_descriptors"));
	assert!(catalog.contains("sync_latest_descriptor_for_tenant"));
	assert!(refresh.contains("self.refreshed_logical_type_registry().await?"));
	assert!(refresh.contains(".list_table_schema_descriptors"));
	assert!(refresh.contains("CatalogError::AdminFailed"));
	assert!(client.contains("pub async fn list_table_schema_descriptors"));
	assert!(wendelon_table.contains("pub fn latest_descriptors_for_tenant"));
	assert!(wendelon_table.contains("latest_by_name"));
	assert!(control.contains(".latest_descriptors_for_tenant("));
	assert!(!control.contains(".descriptors_for_tenant("));
}

#[test]
pub(crate) fn adr102_columns_decode_logical_types_after_refresh() {
	let binder = binder_source();
	let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");
	let column_rows = source_block(&binder, "fn information_schema_column_rows");
	let display = source_block(&binder, "fn logical_type_display_name");

	assert!(bind_relation.contains("refreshed_table_schema_descriptors().await?"));
	assert!(bind_relation.contains("let logical_types = self.catalog.logical_type_registry();"));
	assert!(column_rows.contains(".arrow_schema()"));
	assert!(column_rows.contains("logical_type_display_name(field, logical_types)?"));
	assert!(display.contains("decode_column_logical_type_with_resolver"));
	assert!(display.contains("LogicalTypeResolver::with_registry(logical_types)"));
	assert!(!display.contains("CUSTOMER_ID_LOGICAL_TYPE"));
}

#[test]
pub(crate) fn adr102_system_table_execution_stays_coordinator_local() {
	let execution = execution_source();
	let exec = exec_source();
	let storage = storage_source();
	let pbc = pbc_source();

	assert!(execution.contains("InformationSchemaTables"));
	assert!(execution.contains("InformationSchemaColumns"));
	assert!(execution.contains("InformationSchemaSchemata"));
	assert!(exec.contains("ExecutionPlan::SystemTable { schema, rows, .. }"));
	assert!(exec.contains("materialize_scalar_rows(schema, rows)"));
	for source in [exec, storage, pbc] {
		assert!(!source.contains("list_table_schema_descriptors"));
		assert!(!source.contains("latest_descriptors_for_tenant"));
		assert!(!source.contains("InformationSchemaTables"));
		assert!(!source.contains("InformationSchemaColumns"));
		assert!(!source.contains("InformationSchemaSchemata"));
	}
}

#[test]
pub(crate) fn adr107_schemata_is_descriptor_derived_not_a_registry() {
	let binder = binder_source();
	let bind_relation = source_block(&binder, "async fn bind_information_schema_relation");
	let schema = source_block(&binder, "fn information_schema_schemata_schema");
	let rows = source_block(&binder, "fn information_schema_schemata_rows");

	assert!(
		binder
			.contains("const INFORMATION_SCHEMA_SCHEMATA: &str = \"information_schema.schemata\"")
	);
	assert!(bind_relation.contains("INFORMATION_SCHEMA_SCHEMATA_TABLE"));
	assert!(bind_relation.contains("SystemTableKind::InformationSchemaSchemata"));
	assert!(bind_relation.contains("refreshed_table_schema_descriptors().await?"));
	assert!(schema.contains("Field::new(\n\t\t\"schema_name\",\n\t\tDataType::Utf8,\n\t\tfalse,"));
	assert!(!schema.contains("catalog_name"));
	assert!(!schema.contains("schema_owner"));
	assert!(rows.contains("descriptor.namespace"));
	assert!(rows.contains("BTreeSet"));
	assert!(!rows.contains(".arrow_schema()"));
	assert!(!rows.contains("logical_type_display_name"));
	assert!(!rows.contains("LogicalTypeResolver"));
	assert!(!binder.contains("CREATE SCHEMA"));
	assert!(!binder.contains("SystemTableRegistry"));
}
