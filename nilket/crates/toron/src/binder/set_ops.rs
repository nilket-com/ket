use super::*;

pub(super) fn bind_union(
	left: ScopedExecutionPlan,
	right: ScopedExecutionPlan,
) -> Result<ScopedExecutionPlan, BindingError> {
	let output_schema = set_op_output_schema(&left.schema(), &right.schema())?;
	let left_scope = left.scope.with_schema(output_schema.clone());
	let left_plan = normalize_set_op_arm(left.plan, &output_schema)?;
	let right_plan = normalize_set_op_arm(right.plan, &output_schema)?;
	let join_order_sources = merge_join_order_sources(
		left.join_order_sources.clone(),
		right.join_order_sources.clone(),
	);
	Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
		ExecutionPlan::Union {
			left: Box::new(left_plan),
			right: Box::new(right_plan),
			output_schema,
		},
		left_scope,
		PlanOptimizerFacts::composite_unknown(),
		join_order_sources,
	))
}

pub(super) fn set_op_output_schema(
	left: &SchemaRef,
	right: &SchemaRef,
) -> Result<SchemaRef, BindingError> {
	if left.fields().len() != right.fields().len() {
		return Err(BindingError::UnsupportedExpression {
			message: format!(
				"set operation column count mismatch: left has {}, right has {}",
				left.fields().len(),
				right.fields().len()
			),
		});
	}
	let mut fields = Vec::with_capacity(left.fields().len());
	for (idx, (left_field, right_field)) in
		left.fields().iter().zip(right.fields().iter()).enumerate()
	{
		let data_type = case_common_type(&[
			left_field.data_type().clone(),
			right_field.data_type().clone(),
		])
		.map_err(|_| BindingError::TypeMismatch {
			message: format!(
				"set operation column {} types cannot be unified: {:?} and {:?}",
				idx + 1,
				left_field.data_type(),
				right_field.data_type()
			),
		})?;
		fields.push(field_with_logical_type_metadata(
			left_field.name().clone(),
			data_type,
			left_field.is_nullable() || right_field.is_nullable(),
			LogicalTypeId::NAKED,
		));
	}
	Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn normalize_set_op_arm(
	input: ExecutionPlan,
	output_schema: &SchemaRef,
) -> Result<ExecutionPlan, BindingError> {
	let input_schema = input.schema();
	let mut builder = BoundExprProgramBuilder::new();
	for (idx, output_field) in output_schema.fields().iter().enumerate() {
		let input_field = input_schema.field(idx);
		let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
		let column = builder.append_column(ResolvedColumn {
			name: ColumnName(input_field.name().clone()),
			ordinal,
			data_type: input_field.data_type().clone(),
			nullable: input_field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(input_field.data_type()),
		});
		let column = cast_if_needed(&mut builder, column, output_field.data_type())?;
		builder.push_root(column);
	}
	let exprs = builder.build();
	Ok(ExecutionPlan::Project {
		input: Box::new(input),
		exprs,
		output_schema: output_schema.clone(),
	})
}
