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

pub(super) fn all_output_columns_program(
	schema: &SchemaRef,
) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	for (idx, field) in schema.fields().iter().enumerate() {
		let logical_type = decode_column_logical_type(field).map_err(|error| {
			BindingError::UnsupportedExpression {
				message: error.to_string(),
			}
		})?;
		let ordinal = ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow"));
		let node = builder.append_column(ResolvedColumn {
			name: ColumnName(field.name().clone()),
			ordinal,
			data_type: field.data_type().clone(),
			nullable: field.is_nullable(),
			pbc_eligible: is_pbc_eligible_type(field.data_type()),
		});
		if !builder.set_node_logical_type(node, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "distinct column target is malformed".to_string(),
			});
		}
		builder.push_root(node);
	}
	let program = builder.build();
	program
		.validate()
		.map_err(|error| BindingError::UnsupportedExpression {
			message: format!("distinct group-key program is malformed: {error:?}"),
		})?;
	Ok(program)
}

pub(super) fn repeat_count_column(
	program: &BoundExprProgram,
) -> Result<ColumnOrdinal, BindingError> {
	if program.root_count() != 1 {
		return Err(BindingError::UnsupportedExpression {
			message: "REPEAT count requires exactly one expression".to_string(),
		});
	}
	let root = program.roots[0];
	let node =
		program
			.nodes
			.get(root.0 as usize)
			.ok_or_else(|| BindingError::UnsupportedExpression {
				message: "REPEAT count expression is malformed".to_string(),
			})?;
	let BoundExprNode::Column(column) = node else {
		return Err(BindingError::UnsupportedExpression {
			message: "REPEAT count expression must resolve to a column".to_string(),
		});
	};
	if column.data_type != DataType::Int64 {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"REPEAT count column must be Int64, got {:?}",
				column.data_type
			),
		});
	}
	Ok(column.ordinal)
}

pub(super) fn repeat_output_schema(
	input_schema: &SchemaRef,
	count_column: ColumnOrdinal,
) -> Result<SchemaRef, BindingError> {
	let count_idx = usize::try_from(count_column.0).expect("column ordinal fits usize");
	if count_idx >= input_schema.fields().len() {
		return Err(BindingError::UnsupportedExpression {
			message: format!("REPEAT count column ordinal {count_idx} out of bounds"),
		});
	}
	let fields = input_schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(idx, field)| (idx != count_idx).then(|| field.as_ref().clone()))
		.collect::<Vec<_>>();
	Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn distinct_input_unsupported_message(plan: &LogicalPlan) -> Option<&'static str> {
	match plan {
		LogicalPlan::Join { left, right, .. } => distinct_input_unsupported_message(left)
			.or_else(|| distinct_input_unsupported_message(right)),
		// ADR 279: a deferred EXISTS resolves to a semi-join whose output is the outer rows.
		LogicalPlan::DeferredExists { outer, .. } => distinct_input_unsupported_message(outer),
		// ADR 282: a deferred scalar subquery resolves to a LeftOuter whose output is the outer rows.
		LogicalPlan::DeferredScalarSubquery { outer, .. } => {
			distinct_input_unsupported_message(outer)
		}
		LogicalPlan::Union { left, right } => distinct_input_unsupported_message(left)
			.or_else(|| distinct_input_unsupported_message(right)),
		LogicalPlan::SetOpAll { left, right, .. } => distinct_input_unsupported_message(left)
			.or_else(|| distinct_input_unsupported_message(right)),
		LogicalPlan::Repeat { input, .. } => distinct_input_unsupported_message(input),
		LogicalPlan::Aggregate { .. } => Some("DISTINCT over aggregate inputs is not supported"),
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Distinct { input }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Window { input, .. }
		| LogicalPlan::Take { input, .. } => distinct_input_unsupported_message(input),
		LogicalPlan::Scan { .. } | LogicalPlan::ScanNamed { .. } => None,
		// ADR 255: opaque-local — a recursive CTE is a self-contained local fixpoint (and its
		// self-reference a leaf) whose internals are not walked for DISTINCT support in this
		// first cut; conservatively report the shape as unsupported rather than recursing.
		LogicalPlan::RecursiveCte { .. }
		| LogicalPlan::RecursiveRef { .. }
		| LogicalPlan::Explain { .. }
		| LogicalPlan::ExplainAnalyze { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateLogicalType { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Update { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. } => Some("DISTINCT over this plan shape is not supported"),
	}
}

impl<'a> Binder<'a> {
	pub(super) async fn bind_set_op_all(
		&self,
		mut left: ScopedExecutionPlan,
		mut right: ScopedExecutionPlan,
		kind: SetOpAllKind,
		trace: &mut OptimizerTrace,
		render_trace: &mut Option<OptimizerExplainRenderTrace>,
		declared_position: usize,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let output_schema = set_op_output_schema(&left.schema(), &right.schema())?;
		left.plan = normalize_set_op_arm(left.plan, &output_schema)?;
		right.plan = normalize_set_op_arm(right.plan, &output_schema)?;
		left.scope = left.scope.with_schema(output_schema.clone());
		right.scope = right.scope.with_schema(output_schema.clone());

		let group_aliases = (0..output_schema.fields().len())
			.map(|idx| format!("__toron_setop_key_{idx}"))
			.collect::<Vec<_>>();
		let left_alias = TableRef::new("__toron_setop_left");
		let right_alias = TableRef::new("__toron_setop_right");
		let left_count_name = "__toron_setop_left_count".to_string();
		let right_count_name = "__toron_setop_right_count".to_string();

		let left = self.bind_set_op_all_count_arm(
			left,
			&output_schema,
			&group_aliases,
			&left_count_name,
			left_alias.clone(),
		)?;
		let right = self.bind_set_op_all_count_arm(
			right,
			&output_schema,
			&group_aliases,
			&right_count_name,
			right_alias.clone(),
		)?;

		let join_kind = match kind {
			SetOpAllKind::Intersect => JoinKind::Inner,
			SetOpAllKind::Except => JoinKind::LeftOuter,
		};
		let on = group_aliases
			.iter()
			.map(|name| {
				(
					Expr::qualified_column(left_alias.clone(), name.clone()),
					Expr::qualified_column(right_alias.clone(), name.clone()),
				)
			})
			.collect::<Vec<_>>();
		let joined = self.bind_scoped_join(
			left,
			right,
			join_kind,
			&on,
			true,
			JoinStrategyIntent::Auto,
			trace,
			render_trace,
			declared_position,
			None,
		)?;

		let mut project_exprs = group_aliases
			.iter()
			.map(|name| Expr::qualified_column(left_alias.clone(), name.clone()))
			.collect::<Vec<_>>();
		let left_count = Expr::qualified_column(left_alias, left_count_name);
		let right_count = Expr::qualified_column(right_alias, right_count_name);
		let repeat_count = match kind {
			SetOpAllKind::Intersect => Expr::Case {
				when_then: vec![(
					Box::new(Expr::Lt(
						Box::new(left_count.clone()),
						Box::new(right_count.clone()),
					)),
					Box::new(left_count.clone()),
				)],
				else_expr: Some(Box::new(right_count)),
			},
			SetOpAllKind::Except => Expr::Case {
				when_then: vec![
					(
						Box::new(Expr::IsNull(Box::new(right_count.clone()))),
						Box::new(left_count.clone()),
					),
					(
						Box::new(Expr::Gt(
							Box::new(left_count.clone()),
							Box::new(right_count.clone()),
						)),
						Box::new(Expr::Sub(
							Box::new(left_count.clone()),
							Box::new(right_count),
						)),
					),
				],
				else_expr: Some(Box::new(Expr::Literal(ScalarValue::Int64(Some(0))))),
			},
		};
		project_exprs.push(repeat_count);
		let mut output_names = output_schema
			.fields()
			.iter()
			.map(|field| Some(field.name().clone()))
			.collect::<Vec<_>>();
		output_names.push(Some("__toron_setop_repeat_count".to_string()));
		let project_program = self
			.bind_projection_expr_program_scoped(&project_exprs, &joined.schema(), &joined.scope)
			.await?;
		let project_schema = schema_for_exprs_with_output_names(&project_program, &output_names);
		let project_scope = joined.scope.project(&project_program, &project_schema);
		let project = ExecutionPlan::Project {
			input: Box::new(joined.plan),
			exprs: project_program,
			output_schema: project_schema,
		};
		let count_column = ColumnOrdinal(
			u32::try_from(output_schema.fields().len()).expect("column ordinal overflow"),
		);
		let scope = project_scope.with_schema(output_schema.clone());
		let join_order_sources = joined.join_order_sources.clone();
		Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
			ExecutionPlan::Repeat {
				input: Box::new(project),
				count_column,
				output_schema,
			},
			scope,
			PlanOptimizerFacts::composite_unknown(),
			join_order_sources,
		))
	}

	fn bind_set_op_all_count_arm(
		&self,
		input: ScopedExecutionPlan,
		output_schema: &SchemaRef,
		group_aliases: &[String],
		count_alias: &str,
		relation_alias: TableRef,
	) -> Result<ScopedExecutionPlan, BindingError> {
		let group_expr = all_output_columns_program(output_schema)?;
		let counted = self.bind_bound_aggregate(
			input,
			group_expr,
			vec![BoundAggregateCall {
				function: AggregateFunction::CountAll,
				args: BoundExprProgram::empty(),
			}],
		)?;
		let mut project_names = group_aliases.iter().cloned().map(Some).collect::<Vec<_>>();
		project_names.push(Some(count_alias.to_string()));
		let project_program = all_output_columns_program(&counted.schema())?;
		let project_schema = schema_for_exprs_with_output_names(&project_program, &project_names);
		let plan = ExecutionPlan::Project {
			input: Box::new(counted.plan),
			exprs: project_program,
			output_schema: project_schema.clone(),
		};
		Ok(ScopedExecutionPlan::new_with_facts_and_join_order_sources(
			plan,
			RelationScope::alias(relation_alias, &project_schema),
			PlanOptimizerFacts::composite_unknown(),
			counted.join_order_sources,
		))
	}
}
