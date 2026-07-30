use super::*;

// ADR 521: a grouped COUNT over a proof-unique preserved key can count the
// right side first:
//
//   Aggregate[k; COUNT(v)](L LEFT OUTER R)
//     == Project[k, COALESCE(cnt, 0)]
//          (L LEFT OUTER Aggregate[k_r; cnt = COUNT(v)](R))
//
// under the declining gates below. The proof accessor is the only source of
// uniqueness: identity metadata and optimizer estimates are intentionally not
// consulted.

pub(super) struct OuterCountPreaggregationRewrite {
	pub(super) plan: LogicalPlan,
	pub(super) output_schema: SchemaRef,
	pub(super) output_scope: RelationScope,
}

pub(super) fn is_outer_count_contract_project(program: &BoundExprProgram) -> bool {
	if program.roots.len() != 2 {
		return false;
	}
	if !matches!(
		program.node(program.roots[0]),
		Some(BoundExprNode::Column(_))
	) {
		return false;
	}
	let Some(BoundExprNode::Case {
		when_then,
		else_input,
	}) = program.node(program.roots[1])
	else {
		return false;
	};
	let [(condition, zero)] = when_then.as_slice() else {
		return false;
	};
	let Some(BoundExprNode::Unary {
		op: BoundUnaryOp::IsNull,
		input,
	}) = program.node(*condition)
	else {
		return false;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(*zero) else {
		return false;
	};
	let same_count_column = match (program.node(*input), program.node(*else_input)) {
		(Some(BoundExprNode::Column(left)), Some(BoundExprNode::Column(right))) => {
			left.ordinal == right.ordinal
		}
		_ => false,
	};
	same_count_column
		&& matches!(
			program.literals.get(*literal as usize),
			Some(ScalarValue::Int64(Some(0)))
		)
}

fn plain_column(expr: &Expr) -> Option<&ColumnRef> {
	match expr {
		Expr::Column(column) => Some(column),
		_ => None,
	}
}

fn scope_exclusively_owns(
	left: &RelationScope,
	right: &RelationScope,
	column: &ColumnRef,
) -> Result<Option<bool>, BindingError> {
	let left = left.resolve_join_column(column, "outer-count left")?;
	let right = right.resolve_join_column(column, "outer-count right")?;
	match (left.is_some(), right.is_some()) {
		(true, false) => Ok(Some(true)),
		(false, true) => Ok(Some(false)),
		_ => Ok(None),
	}
}

fn deterministic_right_chain(plan: &LogicalPlan) -> bool {
	match plan {
		LogicalPlan::Scan { .. } | LogicalPlan::ScanNamed { .. } => true,
		LogicalPlan::Filter { input, predicate } => {
			predicate.is_deterministic() && deterministic_right_chain(input)
		}
		LogicalPlan::Project { input, exprs, .. } => {
			exprs.iter().all(Expr::is_deterministic) && deterministic_right_chain(input)
		}
		LogicalPlan::Alias { input, .. }
		| LogicalPlan::Distinct { input }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. }
		| LogicalPlan::Take { input, .. } => deterministic_right_chain(input),
		_ => false,
	}
}

fn binding_ordinal_for_name(binding: &TableBinding, name: &str) -> Option<ColumnOrdinal> {
	binding
		.schema
		.fields()
		.iter()
		.position(|field| field.name() == name)
		.and_then(|index| u32::try_from(index).ok())
		.map(ColumnOrdinal)
}

impl<'a> Binder<'a> {
	async fn preserved_key_is_unique(
		&self,
		plan: &LogicalPlan,
		key: &ColumnRef,
	) -> Result<bool, BindingError> {
		let mut plan = plan;
		let mut key = key.clone();
		for _ in 0..32 {
			match plan {
				LogicalPlan::Scan { table, projection } => {
					let binding = self.catalog.resolve_table_id(*table).await?;
					if projection
						.as_ref()
						.is_some_and(|columns| !columns.contains(&key.name.0))
					{
						return Ok(false);
					}
					let Some(ordinal) = binding_ordinal_for_name(&binding, &key.name.0) else {
						return Ok(false);
					};
					return Ok(binding.unique_key_proof(&[ordinal]).is_some());
				}
				LogicalPlan::ScanNamed { table, projection } => {
					if is_information_schema_namespace(table) {
						return Ok(false);
					}
					let binding = self.catalog.resolve_table(table).await?;
					if projection
						.as_ref()
						.is_some_and(|columns| !columns.contains(&key.name.0))
					{
						return Ok(false);
					}
					let Some(ordinal) = binding_ordinal_for_name(&binding, &key.name.0) else {
						return Ok(false);
					};
					return Ok(binding.unique_key_proof(&[ordinal]).is_some());
				}
				LogicalPlan::Filter { input, .. }
				| LogicalPlan::Distinct { input }
				| LogicalPlan::Exchange { input, .. }
				| LogicalPlan::Sort { input, .. }
				| LogicalPlan::Take { input, .. } => plan = input,
				LogicalPlan::Alias { input, columns, .. } => {
					let Some(scope) = self.logical_scope_for_outer_demotion(plan).await? else {
						return Ok(false);
					};
					let Some(resolved) = scope.resolve_join_column(&key, "outer-count alias")?
					else {
						return Ok(false);
					};
					let index = usize::try_from(resolved.column.ordinal.0)
						.expect("column ordinal fits usize");
					let Some(input_scope) = self.logical_scope_for_outer_demotion(input).await?
					else {
						return Ok(false);
					};
					let Some(field) = input_scope.schema.fields().get(index) else {
						return Ok(false);
					};
					if !columns.is_empty() && columns.get(index).is_none() {
						return Ok(false);
					}
					key = ColumnRef::unqualified(field.name().clone());
					plan = input;
				}
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					let Some(scope) = self.logical_scope_for_outer_demotion(plan).await? else {
						return Ok(false);
					};
					let Some(resolved) = scope.resolve_join_column(&key, "outer-count project")?
					else {
						return Ok(false);
					};
					let index = usize::try_from(resolved.column.ordinal.0)
						.expect("column ordinal fits usize");
					let Some(source) = exprs.get(index).and_then(plain_column) else {
						return Ok(false);
					};
					if output_names.len() != exprs.len() {
						return Ok(false);
					}
					key = source.clone();
					plan = input;
				}
				_ => return Ok(false),
			}
		}
		Ok(false)
	}

	pub(super) async fn try_outer_count_preaggregation(
		&self,
		input: &LogicalPlan,
		group_expr: &[Expr],
		aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	) -> Result<Option<OuterCountPreaggregationRewrite>, BindingError> {
		if !self.config.outer_count_preaggregation || group_expr.len() != 1 || aggr_expr.len() != 1
		{
			return Ok(None);
		}
		let (AggregateFunction::Count, count_args) = &aggr_expr[0] else {
			return Ok(None);
		};
		let [count_arg] = count_args.as_slice() else {
			return Ok(None);
		};
		let Some(count_column) = plain_column(count_arg) else {
			return Ok(None);
		};
		let Some(group_column) = plain_column(&group_expr[0]) else {
			return Ok(None);
		};
		let LogicalPlan::Exchange {
			input: join,
			partition_count,
			key: exchange_key,
		} = input
		else {
			return Ok(None);
		};
		if exchange_key.len() != 1 || plain_column(&exchange_key[0]) != Some(group_column) {
			return Ok(None);
		}
		let LogicalPlan::Join {
			left,
			right,
			kind: JoinKind::LeftOuter,
			on,
			on_filter: None,
			null_safe_keys: false,
			strategy_intent,
		} = join.as_ref()
		else {
			return Ok(None);
		};
		let [(first_key, second_key)] = on.as_slice() else {
			return Ok(None);
		};
		let (Some(first_column), Some(second_column)) =
			(plain_column(first_key), plain_column(second_key))
		else {
			return Ok(None);
		};
		let (Some(left_scope), Some(right_scope)) = (
			self.logical_scope_for_outer_demotion(left).await?,
			self.logical_scope_for_outer_demotion(right).await?,
		) else {
			return Ok(None);
		};
		let (left_key, right_key) = match (
			scope_exclusively_owns(&left_scope, &right_scope, first_column)?,
			scope_exclusively_owns(&left_scope, &right_scope, second_column)?,
		) {
			(Some(true), Some(false)) => (first_column.clone(), second_column.clone()),
			(Some(false), Some(true)) => (second_column.clone(), first_column.clone()),
			_ => return Ok(None),
		};
		if scope_exclusively_owns(&left_scope, &right_scope, group_column)? != Some(true)
			|| scope_exclusively_owns(&left_scope, &right_scope, count_column)? != Some(false)
		{
			return Ok(None);
		}
		let Some(resolved_group) =
			left_scope.resolve_join_column(group_column, "outer-count group key")?
		else {
			return Ok(None);
		};
		let Some(resolved_left_key) =
			left_scope.resolve_join_column(&left_key, "outer-count preserved key")?
		else {
			return Ok(None);
		};
		if resolved_group.column.ordinal != resolved_left_key.column.ordinal
			|| !self.preserved_key_is_unique(left, &left_key).await?
			|| !deterministic_right_chain(right)
		{
			return Ok(None);
		}
		let Some(resolved_count) =
			right_scope.resolve_join_column(count_column, "outer-count counted column")?
		else {
			return Ok(None);
		};
		if resolved_count.column.nullable {
			return Ok(None);
		}

		let Some(original_input_scope) = self.logical_scope_for_outer_demotion(input).await? else {
			return Ok(None);
		};
		let original_group = self
			.bind_expr_program_scoped(
				group_expr,
				&original_input_scope.schema,
				&original_input_scope,
			)
			.await?;
		let original_count = self
			.bind_aggregate_call_scoped(
				&AggregateFunction::Count,
				count_args,
				&original_input_scope.schema,
				&original_input_scope,
			)
			.await?;
		let output_schema = aggregate_schema(
			&original_group,
			std::slice::from_ref(&original_count),
			&self.catalog.logical_type_registry(),
		)?;
		let output_scope = original_input_scope.aggregate(&original_group, &output_schema);

		let alias = TableRef::new("__toron_outer_counts");
		let aliased_key = Expr::qualified_column(alias.clone(), "__toron_outer_count_key");
		let aliased_count = Expr::qualified_column(alias.clone(), "__toron_outer_count_value");
		let right_preaggregate = LogicalPlan::Alias {
			input: Box::new(LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: right.clone(),
					partition_count: *partition_count,
					key: vec![Expr::Column(right_key.clone())],
				}),
				group_expr: vec![Expr::Column(right_key.clone())],
				aggr_expr: vec![(AggregateFunction::Count, vec![count_arg.clone()])],
			}),
			alias: alias.clone(),
			columns: vec![
				"__toron_outer_count_key".to_string(),
				"__toron_outer_count_value".to_string(),
			],
		};
		let joined = LogicalPlan::Join {
			left: left.clone(),
			right: Box::new(right_preaggregate),
			kind: JoinKind::LeftOuter,
			on: vec![(Expr::Column(left_key.clone()), aliased_key)],
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: *strategy_intent,
		};
		let coalesced_count = Expr::Case {
			when_then: vec![(
				Box::new(Expr::IsNull(Box::new(aliased_count.clone()))),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
			)],
			else_expr: Some(Box::new(aliased_count)),
		};
		let plan = LogicalPlan::Project {
			input: Box::new(joined),
			exprs: vec![Expr::Column(left_key), coalesced_count],
			output_names: output_schema
				.fields()
				.iter()
				.map(|field| Some(field.name().clone()))
				.collect(),
		};
		Ok(Some(OuterCountPreaggregationRewrite {
			plan,
			output_schema,
			output_scope,
		}))
	}
}
