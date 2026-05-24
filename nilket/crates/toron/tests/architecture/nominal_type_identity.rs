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

fn error_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error source")
}

fn rune_engine_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/rune_engine.rs"))
		.expect("read rune engine source")
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
