use super::support::*;

#[test]
pub(crate) fn adr173_converted_linear_predicate_is_derived_before_pbc() {
	let binder = source("src/binder.rs");
	let pushdown_plan = first_block(&binder, "fn predicate_pushdown_plan");
	let derived = first_block(&binder, "fn converted_linear_pushdown_predicate");
	let derived_program = first_block(&binder, "fn derived_linear_converted_predicate");

	assert!(pushdown_plan.contains(
		"let derived_predicate = converted_linear_pushdown_predicate(&predicate, logical_types)"
	));
	assert!(
		pushdown_plan.contains("combine_pushdown_predicates(pushed_predicate, derived_predicate)")
	);
	assert!(pushdown_plan.contains("let Some(pbc_program) = compile_pbc(&pushed_predicate)"));
	assert!(derived.contains("collect_top_level_bound_and(predicate, root, &mut conjuncts)"));
	assert!(
		derived.contains("match_linear_converted_comparison(predicate, conjunct, logical_types)")
	);
	assert!(derived.contains("derived_linear_converted_predicate(&comparison)"));
	assert!(derived_program.contains("builder.append_column(comparison.column.clone())"));
	assert!(
		derived_program
			.contains("builder.set_node_logical_type(column, comparison.source_logical)")
	);
	assert!(
		derived_program.contains("builder.append_literal(ScalarValue::Float64(Some(threshold)))")
	);
}

#[test]
pub(crate) fn adr173_converted_pushdown_is_metadata_authenticated() {
	let binder = source("src/binder.rs");
	let matcher = first_block(&binder, "fn match_linear_converted_comparison_sides");
	let mul = first_block(&binder, "fn match_linear_converted_mul");
	let mul_sides = first_block(&binder, "fn match_linear_converted_mul_sides");
	let transform = first_block(&binder, "fn logical_unit_convert_transform");

	assert!(matcher.contains("program.node_logical_type(converted_side)"));
	assert!(matcher.contains("program.node_logical_type(column_id)"));
	assert!(
		matcher.contains("LogicalTypeResolver::with_registry(logical_types).spec(source_logical)")
	);
	assert!(
		matcher.contains("LogicalTypeResolver::with_registry(logical_types).spec(target_logical)")
	);
	assert!(matcher.contains("source_spec.physical != DataType::Float64"));
	assert!(matcher.contains("target_spec.physical != DataType::Float64"));
	assert!(matcher.contains("source_unit.quantity_kind != UnitQuantityKind::Delta"));
	assert!(matcher.contains("target_unit.quantity_kind != UnitQuantityKind::Delta"));
	assert!(matcher.contains("source_unit.dimension != target_unit.dimension"));
	assert!(matcher.contains("logical_unit_convert_transform(source_unit, target_unit)"));
	assert!(matcher.contains("transform.shift != 0.0"));
	assert!(matcher.contains("factor.to_bits() != transform.factor.to_bits()"));
	assert!(mul.contains("op: BoundBinaryOp::Mul"));
	assert!(mul_sides.contains("!column.pbc_eligible || column.data_type != DataType::Float64"));
	assert!(mul_sides.contains("factor <= 0.0"));
	assert!(transform.contains("unit_scale_to_f64(&source_unit.scale_to_base)"));
	assert!(transform.contains("unit_scale_to_f64(&target_unit.scale_to_base)"));
	assert!(transform.contains("unit_offset_to_f64(&source_unit.offset_to_base)"));
	assert!(transform.contains("unit_offset_to_f64(&target_unit.offset_to_base)"));
}

#[test]
pub(crate) fn adr173_converted_pushdown_keeps_original_residual_exact() {
	let binder = source("src/binder.rs");
	let pushdown_plan = first_block(&binder, "fn predicate_pushdown_plan");
	let filter_helper = first_block(&binder, "fn plan_filter_with_predicate_pushdown");
	let partitioner = first_block(&binder, "fn partition_bound_predicate");

	assert!(pushdown_plan.contains("residual_predicate.or(Some(predicate))"));
	assert!(pushdown_plan.contains("residual_predicate: Some(predicate)"));
	assert!(filter_helper.contains("pushdown.residual_predicate"));
	assert!(filter_helper.contains("FilterDisposition::EvaluateInToron"));
	assert!(
		!partitioner.contains("converted_linear"),
		"derived converted predicates must not be removed from the exact residual splitter"
	);
}

#[test]
pub(crate) fn adr173_range_selectivity_uses_converted_source_atoms() {
	let binder = source("src/binder.rs");
	let range = first_block(&binder, "fn filter_range_selectivity_estimated_row_count");
	let converted_atom = first_block(&binder, "fn converted_linear_range_atom");

	assert!(
		range.contains("converted_linear_range_atom(predicate, conjunct, input, logical_types)")
	);
	assert!(
		converted_atom.contains("match_linear_converted_comparison(program, id, logical_types)")
	);
	assert!(converted_atom.contains("widened_converted_linear_threshold"));
	assert!(converted_atom.contains("column_range_for_input(input, comparison.column.ordinal)"));
	assert!(converted_atom.contains("range_scalar_for_fact("));
	assert!(converted_atom.contains("ScalarValue::Float64(Some(threshold))"));
	assert!(converted_atom.contains("op: comparison.op"));
}

#[test]
pub(crate) fn adr173_thresholds_are_widened_outward_by_one_float() {
	let binder = source("src/binder.rs");
	let widening = first_block(&binder, "fn widened_converted_linear_threshold");
	let next_up = first_block(&binder, "fn next_float_up");
	let next_down = first_block(&binder, "fn next_float_down");

	assert!(widening.contains("let threshold = constant / factor"));
	assert!(
		widening.contains("BoundBinaryOp::Gt | BoundBinaryOp::Gte => next_float_down(threshold)")
	);
	assert!(
		widening.contains("BoundBinaryOp::Lt | BoundBinaryOp::Lte => next_float_up(threshold)")
	);
	assert!(widening.contains("widened.is_finite().then_some(widened)"));
	assert!(next_up.contains("f64::from_bits(bits + 1)"));
	assert!(next_down.contains("f64::from_bits(bits - 1)"));
}

#[test]
pub(crate) fn adr173_affine_equality_and_nonconvert_arithmetic_are_deferred() {
	let binder = source("src/binder.rs");
	let comparison = first_block(&binder, "fn match_linear_converted_comparison");
	let matcher = first_block(&binder, "fn match_linear_converted_comparison_sides");
	let mul = first_block(&binder, "fn match_linear_converted_mul");

	assert!(comparison.contains(
		"BoundBinaryOp::Lt | BoundBinaryOp::Lte | BoundBinaryOp::Gt | BoundBinaryOp::Gte"
	));
	assert!(!comparison.contains("BoundBinaryOp::Eq"));
	assert!(!comparison.contains("BoundBinaryOp::Neq"));
	assert!(matcher.contains("source_unit.quantity_kind != UnitQuantityKind::Delta"));
	assert!(matcher.contains("target_unit.quantity_kind != UnitQuantityKind::Delta"));
	assert!(matcher.contains("transform.shift != 0.0"));
	assert!(mul.contains("op: BoundBinaryOp::Mul"));
}

#[test]
pub(crate) fn adr173_pbc_compiler_stays_arithmetic_free() {
	let pbc = source("src/pbc.rs");
	let compile_node = first_block(&pbc, "fn compile_node");
	let binary_opcode = first_block(&pbc, "fn binary_opcode");

	assert!(compile_node.contains("BoundBinaryOp::Add"));
	assert!(compile_node.contains("BoundBinaryOp::Sub"));
	assert!(compile_node.contains("BoundBinaryOp::Mul"));
	assert!(compile_node.contains("BoundBinaryOp::Div"));
	assert!(compile_node.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(binary_opcode.contains("BoundBinaryOp::Add"));
	assert!(binary_opcode.contains("BoundBinaryOp::Sub"));
	assert!(binary_opcode.contains("BoundBinaryOp::Mul"));
	assert!(binary_opcode.contains("BoundBinaryOp::Div"));
	assert!(binary_opcode.contains("arithmetic, Cast, and TryCast are not PBC-eligible"));
	assert!(!pbc.contains("SqlConvert"));
	assert!(!pbc.contains("OP_CONVERT"));
}

#[test]
pub(crate) fn adr173_no_new_storage_or_wendelon_wire_surface() {
	let toron_storage = source("src/storage.rs");
	let wendelon_offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("SqlConvert") || line.contains("OP_CONVERT"))
		.collect::<Vec<_>>();

	assert!(!toron_storage.contains("SqlConvert"));
	assert!(!toron_storage.contains("OP_CONVERT"));
	assert!(
		wendelon_offenders.is_empty(),
		"converted predicate pushdown leaked to Wendelon:\n{}",
		format_offenders(&wendelon_manifest_dir(), &wendelon_offenders)
	);
}

#[test]
pub(crate) fn adr173_converted_pushdown_is_frontend_shared_bound_ir() {
	let binder = source("src/binder.rs");
	let pushdown_plan = first_block(&binder, "fn predicate_pushdown_plan");
	let comparison = first_block(&binder, "fn match_linear_converted_comparison");
	let mul_sides = first_block(&binder, "fn match_linear_converted_mul_sides");
	let tests = first_block(
		&binder,
		"async fn converted_linear_predicate_pushes_as_bare_column",
	);

	assert!(binder.contains("fn converted_linear_pushdown_predicate("));
	assert!(binder.contains("predicate: &BoundExprProgram"));
	assert!(comparison.contains("BoundExprNode::Binary"));
	assert!(mul_sides.contains("BoundExprNode::Column"));
	assert!(!pushdown_plan.contains("SqlConvert"));
	assert!(!comparison.contains("SqlConvert"));
	assert!(!mul_sides.contains("SqlConvert"));
	assert!(tests.contains("CONVERT(distance_mile, Kilometer)"));
}

fn source(relative: &str) -> String {
	std::fs::read_to_string(manifest_dir().join(relative)).expect("read Toron source")
}

fn first_block(source: &str, marker: &str) -> String {
	brace_blocks_after_marker(source, marker)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("missing block for {marker}"))
		.1
}
