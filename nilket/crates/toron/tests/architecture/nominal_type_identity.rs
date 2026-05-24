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
	let decoder = source_block(&ir, "pub fn decode_column_logical_type");

	assert!(decoder.contains("field.metadata().get(LOGICAL_TYPE_ID_METADATA_KEY)"));
	assert!(decoder.contains("parse::<u32>()"));
	assert!(decoder.contains("LogicalTypeId::NAKED"));
	assert!(decoder.contains("logical_type_spec(id)"));
	assert!(decoder.contains("spec.physical != *field.data_type()"));
	assert!(ir.contains("LogicalTypeMetadataError::InvalidDecimal"));
	assert!(ir.contains("LogicalTypeMetadataError::UnknownId"));
	assert!(ir.contains("LogicalTypeMetadataError::PhysicalMismatch"));
}

#[test]
pub(crate) fn adr088_descriptor_binding_validates_declared_logical_metadata() {
	let catalog = catalog_source();
	let from_descriptor = source_block(&catalog, "pub fn from_descriptor");

	let arrow_schema = from_descriptor
		.find("descriptor.arrow_schema()")
		.expect("descriptor schema decode");
	let validate = from_descriptor
		.find("validate_logical_column_metadata(schema.as_ref())?")
		.expect("logical metadata validation");
	let row_identity = from_descriptor
		.find("table_row_identity")
		.expect("row identity decode");

	assert!(arrow_schema < validate);
	assert!(validate < row_identity);
	assert!(catalog.contains("fn validate_logical_column_metadata"));
	assert!(catalog.contains("decode_column_logical_type(field)"));
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
