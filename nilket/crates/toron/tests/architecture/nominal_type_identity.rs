#![allow(unused_imports)]

use super::support::*;

fn ir_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR source")
}

fn execution_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source")
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
	let physical_check = comparison
		.find("if left_type != right_type")
		.expect("physical comparison type gate");
	let nominal = comparison
		.find("nominal_compatible")
		.expect("nominal comparison gate");

	assert!(coerce < physical_check);
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
	let from_descriptor = source_block(&catalog, "pub fn from_descriptor_with_logical_types");

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
		.find("resolve_logical_column(&column_ref.name.0, input_schema)?")
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
	let resolve = source_block(&binder, "fn resolve_logical_column");
	let join_key = source_block(&binder, "fn resolved_join_key");

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
pub(crate) fn adr089_group_keys_preserve_identity_aggregate_outputs_naked() {
	let binder = binder_source();
	let aggregate_schema = source_block(&binder, "fn aggregate_schema");
	let aggregate_field = source_block(&binder, "fn aggregate_field");

	assert!(aggregate_schema.contains("schema_for_exprs(group_expr)"));
	assert!(aggregate_schema.contains("fields.push(aggregate_field(call))"));
	assert!(!aggregate_schema.contains("field_with_logical_type_metadata"));
	assert!(!aggregate_schema.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(!aggregate_field.contains("field_with_logical_type_metadata"));
	assert!(!aggregate_field.contains("LOGICAL_TYPE_ID_METADATA_KEY"));
	assert!(aggregate_field.contains("Field::new"));
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
	assert!(compute.contains("pub struct LogicalTypeDefinitionV1"));
	assert!(raft.contains("pub logical_types: LogicalTypeCatalogV1"));
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
pub(crate) fn adr090_no_refinement_dropalter_or_pkfk_derivation() {
	let compute = wendelon_compute_source();
	let raft = wendelon_raft_storage_source();
	let planner = sql_planner_source();

	assert!(raft.contains("logical type ALTER/DROP is not supported"));
	assert!(!planner.contains("PRIMARY KEY") || !planner.contains("LogicalTypePhysicalV1"));
	assert!(!compute.contains("refinement"));
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
	let lower_logical = source_block(&planner, "fn lower_create_table_logical_type_name");

	assert!(lower_type.contains("SqlDataType::Custom(name, modifiers)"));
	assert!(lower_type.contains("CreateTableColumnType::LogicalName"));
	assert!(lower_logical.contains("ident.quote_style.is_some()"));
	assert!(lower_logical.contains("eq_ignore_ascii_case(\"naked\")"));
	assert!(lower_logical.contains("ident.value.clone()"));
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
pub(crate) fn adr091_no_cast_or_typed_ctas_authoring_yet() {
	let planner = sql_planner_source();
	let cast = source_block(&planner, "fn lower_cast_target_type");
	let ctas = source_block(&planner, "fn lower_create_table_as_select");

	assert!(!cast.contains("SqlDataType::Custom"));
	assert!(ctas.contains("CTAS does not support explicit column lists"));
	assert!(!ctas.contains("CreateTableSchema"));
	assert!(!ctas.contains("LogicalName"));
}
