use super::*;

pub(super) fn bind_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
) -> Result<BoundAggregateCall, BindingError> {
	if matches!(function, AggregateFunction::CountAll) {
		return Ok(BoundAggregateCall {
			function: function.clone(),
			args: BoundExprProgramBuilder::new().build(),
		});
	}
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return bind_stable_aggregate_call(function, args, input_schema, ctx, spec);
	}
	match function {
		AggregateFunction::LinearRegress => {
			if !ctx.config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "LinearRegress",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, ctx))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundAggregateCall {
				function: function.clone(),
				args: merge_aggregate_args(args),
			})
		}
		AggregateFunction::ApproxQuantile(_) => {
			if !ctx.config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "ApproxQuantile",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, ctx))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundAggregateCall {
				function: function.clone(),
				args: merge_aggregate_args(args),
			})
		}
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

pub(super) fn bind_stable_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	spec: &AggregateKernelSpec,
) -> Result<BoundAggregateCall, BindingError> {
	if args.len() != aggregate_arity_len(spec.arity) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("{} requires one argument", spec.sql_name),
		});
	}
	let args = bind_expr_program(args, input_schema, ctx)?;
	if !matches!(spec.input_domain, AggregateInputDomain::Any) {
		let Some(data_type) = args.root_type(0) else {
			return Err(BindingError::UnsupportedExpression {
				message: format!("{} requires one argument", spec.sql_name),
			});
		};
		if aggregate_kernel::stable_output_data_type(function, Some(data_type)).is_none() {
			return Err(BindingError::TypeMismatch {
				message: aggregate_type_mismatch_message(spec.kind, data_type),
			});
		}
	}
	Ok(BoundAggregateCall {
		function: function.clone(),
		args,
	})
}

pub(super) fn merge_aggregate_args(programs: Vec<BoundExprProgram>) -> BoundExprProgram {
	if programs.is_empty() {
		return BoundExprProgramBuilder::new().build();
	}
	if programs.len() == 1 {
		return programs.into_iter().next().expect("one program");
	}
	let mut merged = BoundExprProgram {
		nodes: Vec::new(),
		node_types: Vec::new(),
		node_logical_types: Vec::new(),
		literals: Vec::new(),
		roots: Vec::new(),
	};
	for program in programs {
		let node_offset =
			u32::try_from(merged.nodes.len()).expect("bound expression node offset overflow");
		let literal_offset =
			u32::try_from(merged.literals.len()).expect("bound expression literal offset overflow");
		merged.literals.extend(program.literals);
		merged.node_types.extend(program.node_types);
		merged.node_logical_types.extend(program.node_logical_types);
		merged.nodes.extend(
			program
				.nodes
				.into_iter()
				.map(|node| offset_bound_node(node, node_offset, literal_offset)),
		);
		merged.roots.extend(
			program
				.roots
				.into_iter()
				.map(|root| BoundExprId(root.0 + node_offset)),
		);
	}
	merged
}

pub(super) fn offset_bound_node(
	node: BoundExprNode,
	node_offset: u32,
	literal_offset: u32,
) -> BoundExprNode {
	let offset_id = |id: BoundExprId| BoundExprId(id.0 + node_offset);
	match node {
		BoundExprNode::Column(column) => BoundExprNode::Column(column),
		BoundExprNode::Literal(literal) => BoundExprNode::Literal(literal + literal_offset),
		BoundExprNode::NullLiteral => BoundExprNode::NullLiteral,
		BoundExprNode::Cast { input, target } => BoundExprNode::Cast {
			input: offset_id(input),
			target,
		},
		BoundExprNode::TryCast { input, target } => BoundExprNode::TryCast {
			input: offset_id(input),
			target,
		},
		BoundExprNode::Binary { op, left, right } => BoundExprNode::Binary {
			op,
			left: offset_id(left),
			right: offset_id(right),
		},
		BoundExprNode::Unary { op, input } => BoundExprNode::Unary {
			op,
			input: offset_id(input),
		},
		BoundExprNode::InList {
			expr,
			list,
			negated,
		} => BoundExprNode::InList {
			expr: offset_id(expr),
			list: list.into_iter().map(offset_id).collect(),
			negated,
		},
		BoundExprNode::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => BoundExprNode::Like {
			expr: offset_id(expr),
			pattern,
			negated,
			case_insensitive,
		},
		BoundExprNode::Case {
			when_then,
			else_input,
		} => BoundExprNode::Case {
			when_then: when_then
				.into_iter()
				.map(|(condition, result)| (offset_id(condition), offset_id(result)))
				.collect(),
			else_input: offset_id(else_input),
		},
		BoundExprNode::TimeBucket { interval, input } => BoundExprNode::TimeBucket {
			interval,
			input: offset_id(input),
		},
		BoundExprNode::AtTimeZone { input, zone } => BoundExprNode::AtTimeZone {
			input: offset_id(input),
			zone,
		},
		BoundExprNode::Extract { field, zone, input } => BoundExprNode::Extract {
			field,
			zone,
			input: offset_id(input),
		},
		BoundExprNode::DateTrunc { field, zone, input } => BoundExprNode::DateTrunc {
			field,
			zone,
			input: offset_id(input),
		},
	}
}

pub(super) fn aggregate_arity_len(arity: AggregateArity) -> usize {
	match arity {
		AggregateArity::Zero => 0,
		AggregateArity::One => 1,
	}
}

pub(super) fn aggregate_type_mismatch_message(
	kernel: StableAggregateKernel,
	data_type: &DataType,
) -> String {
	match kernel {
		StableAggregateKernel::Sum => {
			format!("SUM expects numeric input, got {data_type:?}")
		}
		StableAggregateKernel::Avg => {
			format!("AVG expects numeric input, got {data_type:?}")
		}
		StableAggregateKernel::Min => {
			format!("MIN expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
		}
		StableAggregateKernel::Max => {
			format!("MAX expects Int32, Int64, Float32, or Float64 input, got {data_type:?}")
		}
		StableAggregateKernel::CountAll | StableAggregateKernel::Count => {
			unreachable!("COUNT accepts any input type")
		}
	}
}

pub(super) fn aggregate_schema(
	group_expr: &BoundExprProgram,
	aggr_expr: &[BoundAggregateCall],
	logical_types: &LogicalTypeRegistry,
) -> Result<SchemaRef, BindingError> {
	let mut fields = Vec::new();
	let group_schema = schema_for_exprs(group_expr);
	fields.extend(
		group_schema
			.fields()
			.iter()
			.map(|field| field.as_ref().clone()),
	);
	// ADR 267: name the aggregate output fields, then disambiguate multiple same-natural-name aggregates
	// (e.g. several `SUM`s) to a unique `__agg_<i>` internal name. `i` is the aggregate's position in
	// `aggr_expr`, which matches the frontend `AggregateOutputCatalog` entry order, so the projection's
	// rewritten references resolve to these columns. A single aggregate keeps its natural name.
	let agg_fields = aggr_expr
		.iter()
		.map(|call| aggregate_field(call, logical_types))
		.collect::<Result<Vec<_>, _>>()?;
	let mut natural_counts: HashMap<String, usize> = HashMap::new();
	for field in &agg_fields {
		*natural_counts.entry(field.name().clone()).or_insert(0) += 1;
	}
	for (index, field) in agg_fields.into_iter().enumerate() {
		let field = if natural_counts.get(field.name()).copied().unwrap_or(0) > 1 {
			field.with_name(format!("__agg_{index}"))
		} else {
			field
		};
		fields.push(field);
	}
	Ok(Arc::new(Schema::new(fields)))
}

pub(super) fn schema_for_exprs_with_output_names(
	exprs: &BoundExprProgram,
	output_names: &[Option<String>],
) -> SchemaRef {
	debug_assert_eq!(exprs.root_count(), output_names.len());
	let derived = schema_for_exprs(exprs);
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = output_names[idx]
				.as_ref()
				.cloned()
				.unwrap_or_else(|| derived.field(idx).name().clone());
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
			field_with_logical_type_metadata(name, data_type, nullable, logical_type)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

pub(super) fn schema_for_projected_exprs_with_existing_names(
	exprs: &BoundExprProgram,
	existing_schema: &SchemaRef,
	requested_outputs: &[ColumnOrdinal],
) -> SchemaRef {
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = requested_outputs
				.get(idx)
				.and_then(|ordinal| existing_schema.fields().get(ordinal.0 as usize))
				.map(|field| field.name().clone())
				.unwrap_or_else(|| {
					exprs
						.root_display_name(idx)
						.map(str::to_string)
						.unwrap_or_else(|| format!("col_{idx}"))
				});
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			let logical_type = exprs.root_logical_type(idx).unwrap_or(LogicalTypeId::NAKED);
			field_with_logical_type_metadata(name, data_type, nullable, logical_type)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

pub(super) fn aggregate_field(
	call: &BoundAggregateCall,
	logical_types: &LogicalTypeRegistry,
) -> Result<Field, BindingError> {
	let field =
		aggregate_kernel::stable_output_field(call).unwrap_or_else(|| match call.function {
			AggregateFunction::LinearRegress => Field::new("linreg", DataType::Float64, true),
			AggregateFunction::ApproxQuantile(_) => Field::new("quantile", DataType::Float64, true),
			AggregateFunction::CountAll
			| AggregateFunction::Count
			| AggregateFunction::Sum
			| AggregateFunction::Min
			| AggregateFunction::Max
			| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
		});
	let logical_type = aggregate_output_logical_type(call, field.data_type(), logical_types)?;
	if logical_type.is_naked() {
		Ok(field)
	} else {
		Ok(field_with_logical_type_metadata(
			field.name().clone(),
			field.data_type().clone(),
			field.is_nullable(),
			logical_type,
		))
	}
}

pub(super) fn aggregate_output_logical_type(
	call: &BoundAggregateCall,
	output_type: &DataType,
	logical_types: &LogicalTypeRegistry,
) -> Result<LogicalTypeId, BindingError> {
	match call.function {
		AggregateFunction::Min | AggregateFunction::Max => {
			let Some(logical_type) = call.args.root_logical_type(0) else {
				return Ok(LogicalTypeId::NAKED);
			};
			let Some(input_type) = call.args.root_type(0) else {
				return Ok(LogicalTypeId::NAKED);
			};
			if logical_type.is_naked() || input_type != output_type {
				Ok(LogicalTypeId::NAKED)
			} else {
				Ok(logical_type)
			}
		}
		AggregateFunction::Sum | AggregateFunction::Avg => {
			additive_unit_aggregate_logical_type(call, output_type, logical_types)
		}
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::LinearRegress
		| AggregateFunction::ApproxQuantile(_) => Ok(LogicalTypeId::NAKED),
	}
}

pub(super) fn additive_unit_aggregate_logical_type(
	call: &BoundAggregateCall,
	output_type: &DataType,
	logical_types: &LogicalTypeRegistry,
) -> Result<LogicalTypeId, BindingError> {
	let Some(logical_type) = call.args.root_logical_type(0) else {
		return Ok(LogicalTypeId::NAKED);
	};
	let aggregate_name = match call.function {
		AggregateFunction::Sum => "SUM",
		AggregateFunction::Avg => "AVG",
		_ => unreachable!("additive helper is only used by SUM/AVG"),
	};
	match logical_arithmetic_domain_for_type(logical_type, logical_types)? {
		LogicalArithmeticDomain::Naked => Ok(LogicalTypeId::NAKED),
		LogicalArithmeticDomain::Identity(spec) => Err(non_arithmetic_logical_type_error(
			&spec.name,
			aggregate_name,
		)),
		LogicalArithmeticDomain::Quantity(spec) => {
			if quantity_kind_for_spec(&spec) == Some(UnitQuantityKind::Point) {
				if matches!(call.function, AggregateFunction::Avg) {
					let Some(input_type) = call.args.root_type(0) else {
						return Ok(LogicalTypeId::NAKED);
					};
					// ADR 268: AVG(Decimal) widens its output type (scale/precision +4), so input != output
					// and a decimal average is deliberately NAKED — the widened quotient is a plain Decimal,
					// not the column's declared unit, until a logical-type widening policy is designed.
					if input_type == output_type && spec.physical == *output_type {
						return Ok(logical_type);
					}
					return Ok(LogicalTypeId::NAKED);
				}
				return Err(point_arithmetic_error(&spec, aggregate_name));
			}
			let Some(input_type) = call.args.root_type(0) else {
				return Ok(LogicalTypeId::NAKED);
			};
			if input_type == output_type && spec.physical == *output_type {
				Ok(logical_type)
			} else {
				Ok(LogicalTypeId::NAKED)
			}
		}
	}
}
