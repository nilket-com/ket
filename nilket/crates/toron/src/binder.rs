use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::catalog::{Catalog, TableBinding};
use crate::error::BindingError;
use crate::ir::{
	AggregateCall, AggregateFunction, ColumnName, ColumnOrdinal, ExecutionExpr, ExecutionPlan,
	Expr, FilterDisposition, LogicalPlan, PbcProgram, QueryProfileHint, ResolvedColumn,
	ScalarValue, schema_for_exprs,
};
use crate::pbc::PbcCompiler;

#[derive(Debug, Clone, Copy, Default)]
pub struct PlannerConfig {
	pub allow_experimental_kernels: bool,
}

pub async fn bind(
	plan: &LogicalPlan,
	catalog: &dyn Catalog,
	config: &PlannerConfig,
) -> Result<ExecutionPlan, BindingError> {
	let plan = Binder { catalog, config }.bind_plan(plan).await?;
	push_down_scan_projection(plan)
}

struct Binder<'a> {
	catalog: &'a dyn Catalog,
	config: &'a PlannerConfig,
}

impl<'a> Binder<'a> {
	fn bind_plan<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<Box<dyn Future<Output = Result<ExecutionPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Scan { table, projection } => {
					let binding = self.catalog.resolve_table_id(*table).await?;
					let projection = projection
						.as_ref()
						.map(|columns| {
							columns
								.iter()
								.map(|column| {
									resolve_column(column, &binding.schema)
										.map(|column| column.ordinal)
								})
								.collect::<Result<Vec<_>, _>>()
						})
						.transpose()?;

					Ok(scan_plan(binding, projection))
				}
				LogicalPlan::ScanNamed { table, projection } => {
					let binding = self.catalog.resolve_table(table).await?;
					let projection = projection
						.as_ref()
						.map(|columns| {
							columns
								.iter()
								.map(|column| {
									resolve_column(column, &binding.schema)
										.map(|column| column.ordinal)
								})
								.collect::<Result<Vec<_>, _>>()
						})
						.transpose()?;

					Ok(scan_plan(binding, projection))
				}
				LogicalPlan::Filter { input, predicate } => {
					let mut input = self.bind_plan(input).await?;
					let bound_predicate = bind_expr(predicate, &input.schema(), self.config)?;
					let profile_hint = scan_profile_hint(&bound_predicate, &input);
					apply_scan_profile_hint(&mut input, profile_hint);
					let (input, disposition) = if bound_predicate.pbc_eligible() {
						match compile_pbc(&bound_predicate) {
							Some(program) => match attach_pbc_program(input, program) {
								Ok(input) => (input, FilterDisposition::FullyPushed),
								Err(input) => (input, FilterDisposition::EvaluateInToron),
							},
							None => (input, FilterDisposition::EvaluateInToron),
						}
					} else {
						(input, FilterDisposition::EvaluateInToron)
					};

					Ok(ExecutionPlan::Filter {
						input: Box::new(input),
						predicate: bound_predicate,
						disposition,
					})
				}
				LogicalPlan::Project { input, exprs } => {
					let input = self.bind_plan(input).await?;
					let exprs = exprs
						.iter()
						.map(|expr| bind_expr(expr, &input.schema(), self.config))
						.collect::<Result<Vec<_>, _>>()?;
					let output_schema = schema_for_exprs(&exprs);

					Ok(ExecutionPlan::Project {
						input: Box::new(input),
						exprs,
						output_schema,
					})
				}
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					let input = self.bind_plan(input).await?;
					let input_schema = input.schema();
					let group_expr = group_expr
						.iter()
						.map(|expr| bind_expr(expr, &input_schema, self.config))
						.collect::<Result<Vec<_>, _>>()?;
					let aggr_expr = aggr_expr
						.iter()
						.map(|(function, args)| {
							bind_aggregate_call(function, args, &input_schema, self.config)
						})
						.collect::<Result<Vec<_>, _>>()?;
					let output_schema = aggregate_schema(&group_expr, &aggr_expr);

					Ok(ExecutionPlan::Aggregate {
						input: Box::new(input),
						group_expr,
						aggr_expr,
						output_schema,
					})
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let input = self.bind_plan(input).await?;
					let input_schema = input.schema();
					let key = key
						.iter()
						.map(|expr| bind_expr(expr, &input_schema, self.config))
						.collect::<Result<Vec<_>, _>>()?;

					Ok(ExecutionPlan::Exchange {
						input: Box::new(input),
						partition_count: *partition_count,
						key,
					})
				}
			}
		})
	}
}

fn compile_pbc(predicate: &ExecutionExpr) -> Option<PbcProgram> {
	PbcCompiler::new()
		.compile(Some(predicate), None)
		.ok()
		.map(PbcProgram)
}

fn attach_pbc_program(
	mut plan: ExecutionPlan,
	program: PbcProgram,
) -> Result<ExecutionPlan, ExecutionPlan> {
	match &mut plan {
		ExecutionPlan::Scan { pbc_program, .. } => {
			*pbc_program = Some(program);
			Ok(plan)
		}
		_ => Err(plan),
	}
}

fn scan_plan(binding: TableBinding, projection: Option<Vec<ColumnOrdinal>>) -> ExecutionPlan {
	ExecutionPlan::Scan {
		legacy_table_id: binding.legacy_table_id,
		table: binding.table,
		fingerprint: binding.fingerprint,
		schema: binding.schema,
		projection,
		pbc_program: None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: sorted_columns(&binding.primary_key_columns),
	}
}

fn sorted_columns(columns: &HashSet<ColumnOrdinal>) -> Vec<ColumnOrdinal> {
	let mut columns = columns.iter().copied().collect::<Vec<_>>();
	columns.sort();
	columns
}

fn scan_profile_hint(predicate: &ExecutionExpr, input: &ExecutionPlan) -> Option<QueryProfileHint> {
	match input {
		ExecutionPlan::Scan {
			primary_key_columns,
			..
		} => {
			if predicate_includes_primary_key_equality(predicate, primary_key_columns) {
				Some(QueryProfileHint::PointRead)
			} else {
				Some(QueryProfileHint::Scan)
			}
		}
		_ => None,
	}
}

fn apply_scan_profile_hint(plan: &mut ExecutionPlan, profile_hint: Option<QueryProfileHint>) {
	if let Some(next_profile_hint) = profile_hint {
		if let ExecutionPlan::Scan { profile_hint, .. } = plan {
			*profile_hint = next_profile_hint;
		}
	}
}

fn predicate_includes_primary_key_equality(
	predicate: &ExecutionExpr,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	match predicate {
		ExecutionExpr::Eq(left, right) => {
			primary_key_literal_equality(left, right, primary_key_columns)
				|| primary_key_literal_equality(right, left, primary_key_columns)
		}
		ExecutionExpr::And(left, right) => {
			predicate_includes_primary_key_equality(left, primary_key_columns)
				|| predicate_includes_primary_key_equality(right, primary_key_columns)
		}
		_ => false,
	}
}

fn primary_key_literal_equality(
	column_side: &ExecutionExpr,
	literal_side: &ExecutionExpr,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	match (column_side, literal_side) {
		(ExecutionExpr::Column(column), ExecutionExpr::Literal(value)) => {
			primary_key_columns.contains(&column.ordinal) && literal_is_non_null(value)
		}
		_ => false,
	}
}

fn literal_is_non_null(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Boolean(value) => value.is_some(),
		ScalarValue::Int32(value) => value.is_some(),
		ScalarValue::Int64(value) => value.is_some(),
		ScalarValue::Float32(value) => value.is_some(),
		ScalarValue::Float64(value) => value.is_some(),
		ScalarValue::Utf8(value) => value.is_some(),
	}
}

type OrdinalRemap = HashMap<ColumnOrdinal, ColumnOrdinal>;

fn push_down_scan_projection(plan: ExecutionPlan) -> Result<ExecutionPlan, BindingError> {
	let (plan, _) = optimize_for_required(plan, None)?;
	Ok(plan)
}

fn optimize_for_required(
	plan: ExecutionPlan,
	required_output: Option<BTreeSet<ColumnOrdinal>>,
) -> Result<(ExecutionPlan, OrdinalRemap), BindingError> {
	match plan {
		ExecutionPlan::Scan {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			projection: _,
			pbc_program,
			profile_hint,
			primary_key_columns,
		} => {
			let all_columns = all_columns(&schema);
			let required = required_output.unwrap_or(all_columns);
			let requested = required.iter().copied().collect::<Vec<_>>();
			let remap = ordinal_remap(&requested);
			let schema = project_schema(&schema, &requested)?;
			let primary_key_columns = primary_key_columns
				.into_iter()
				.filter_map(|column| remap.get(&column).copied())
				.collect();

			Ok((
				ExecutionPlan::Scan {
					legacy_table_id,
					table,
					fingerprint,
					schema,
					projection: Some(requested),
					pbc_program,
					profile_hint,
					primary_key_columns,
				},
				remap,
			))
		}
		ExecutionPlan::Filter {
			input,
			predicate,
			disposition,
		} => {
			let output_required = required_output.unwrap_or_else(|| all_columns(&input.schema()));
			let mut child_required = output_required.clone();
			collect_expr_columns(&predicate, &mut child_required);
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let predicate = rewrite_expr_ordinals(predicate, &remap)?;

			Ok((
				ExecutionPlan::Filter {
					input: Box::new(input),
					predicate,
					disposition,
				},
				remap,
			))
		}
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema: _,
		} => {
			let requested_outputs = required_output.unwrap_or_else(|| output_columns(exprs.len()));
			let mut child_required = BTreeSet::new();
			for ordinal in &requested_outputs {
				let expr = exprs.get(ordinal.0 as usize).ok_or_else(|| {
					BindingError::UnsupportedExpression {
						message: format!("project output ordinal {:?} out of bounds", ordinal),
					}
				})?;
				collect_expr_columns(expr, &mut child_required);
			}
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let exprs = exprs
				.into_iter()
				.map(|expr| rewrite_expr_ordinals(expr, &remap))
				.collect::<Result<Vec<_>, _>>()?;
			let output_schema = schema_for_exprs(&exprs);
			let output_remap =
				ordinal_remap(&requested_outputs.iter().copied().collect::<Vec<_>>());

			Ok((
				ExecutionPlan::Project {
					input: Box::new(input),
					exprs,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Aggregate {
			input,
			group_expr,
			aggr_expr,
			output_schema: _,
		} => {
			let mut child_required = BTreeSet::new();
			for expr in &group_expr {
				collect_expr_columns(expr, &mut child_required);
			}
			for call in &aggr_expr {
				for expr in &call.args {
					collect_expr_columns(expr, &mut child_required);
				}
			}

			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let group_expr = group_expr
				.into_iter()
				.map(|expr| rewrite_expr_ordinals(expr, &remap))
				.collect::<Result<Vec<_>, _>>()?;
			let aggr_expr = aggr_expr
				.into_iter()
				.map(|call| {
					Ok(AggregateCall {
						function: call.function,
						args: call
							.args
							.into_iter()
							.map(|expr| rewrite_expr_ordinals(expr, &remap))
							.collect::<Result<Vec<_>, _>>()?,
					})
				})
				.collect::<Result<Vec<_>, BindingError>>()?;
			let output_schema = aggregate_schema(&group_expr, &aggr_expr);
			let output_remap = ordinal_remap(
				&required_output
					.unwrap_or_else(|| output_columns(output_schema.fields().len()))
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);

			Ok((
				ExecutionPlan::Aggregate {
					input: Box::new(input),
					group_expr,
					aggr_expr,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Exchange {
			input,
			partition_count,
			key,
		} => {
			let output_required = required_output.unwrap_or_else(|| all_columns(&input.schema()));
			let mut child_required = output_required.clone();
			for expr in &key {
				collect_expr_columns(expr, &mut child_required);
			}
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let key = key
				.into_iter()
				.map(|expr| rewrite_expr_ordinals(expr, &remap))
				.collect::<Result<Vec<_>, _>>()?;
			Ok((
				ExecutionPlan::Exchange {
					input: Box::new(input),
					partition_count,
					key,
				},
				remap,
			))
		}
	}
}

fn all_columns(schema: &SchemaRef) -> BTreeSet<ColumnOrdinal> {
	output_columns(schema.fields().len())
}

fn output_columns(len: usize) -> BTreeSet<ColumnOrdinal> {
	(0..len)
		.map(|idx| ColumnOrdinal(u32::try_from(idx).expect("column ordinal overflow")))
		.collect()
}

fn ordinal_remap(requested: &[ColumnOrdinal]) -> OrdinalRemap {
	requested
		.iter()
		.enumerate()
		.map(|(new, old)| {
			(
				*old,
				ColumnOrdinal(u32::try_from(new).expect("column ordinal overflow")),
			)
		})
		.collect()
}

fn project_schema(
	schema: &SchemaRef,
	requested: &[ColumnOrdinal],
) -> Result<SchemaRef, BindingError> {
	let fields = requested
		.iter()
		.map(|ordinal| {
			schema
				.fields()
				.get(ordinal.0 as usize)
				.map(|field| field.as_ref().clone())
				.ok_or_else(|| BindingError::UnknownColumn {
					column: format!("ordinal {}", ordinal.0),
				})
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Schema::new(fields)))
}

fn collect_expr_columns(expr: &ExecutionExpr, columns: &mut BTreeSet<ColumnOrdinal>) {
	match expr {
		ExecutionExpr::Column(column) => {
			columns.insert(column.ordinal);
		}
		ExecutionExpr::Literal(_) => {}
		ExecutionExpr::Eq(left, right)
		| ExecutionExpr::Neq(left, right)
		| ExecutionExpr::Lt(left, right)
		| ExecutionExpr::Lte(left, right)
		| ExecutionExpr::Gt(left, right)
		| ExecutionExpr::Gte(left, right)
		| ExecutionExpr::And(left, right)
		| ExecutionExpr::Or(left, right) => {
			collect_expr_columns(left, columns);
			collect_expr_columns(right, columns);
		}
		ExecutionExpr::Not(inner)
		| ExecutionExpr::IsNull(inner)
		| ExecutionExpr::IsNotNull(inner)
		| ExecutionExpr::TimeBucket(_, inner) => collect_expr_columns(inner, columns),
	}
}

fn rewrite_expr_ordinals(
	expr: ExecutionExpr,
	remap: &OrdinalRemap,
) -> Result<ExecutionExpr, BindingError> {
	match expr {
		ExecutionExpr::Column(mut column) => {
			column.ordinal =
				*remap
					.get(&column.ordinal)
					.ok_or_else(|| BindingError::UnsupportedExpression {
						message: format!("missing ordinal remap for {:?}", column.ordinal),
					})?;
			Ok(ExecutionExpr::Column(column))
		}
		ExecutionExpr::Literal(value) => Ok(ExecutionExpr::Literal(value)),
		ExecutionExpr::Eq(left, right) => Ok(ExecutionExpr::Eq(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Neq(left, right) => Ok(ExecutionExpr::Neq(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Lt(left, right) => Ok(ExecutionExpr::Lt(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Lte(left, right) => Ok(ExecutionExpr::Lte(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Gt(left, right) => Ok(ExecutionExpr::Gt(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Gte(left, right) => Ok(ExecutionExpr::Gte(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::And(left, right) => Ok(ExecutionExpr::And(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Or(left, right) => Ok(ExecutionExpr::Or(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Not(inner) => Ok(ExecutionExpr::Not(Box::new(rewrite_expr_ordinals(
			*inner, remap,
		)?))),
		ExecutionExpr::IsNull(inner) => Ok(ExecutionExpr::IsNull(Box::new(rewrite_expr_ordinals(
			*inner, remap,
		)?))),
		ExecutionExpr::IsNotNull(inner) => Ok(ExecutionExpr::IsNotNull(Box::new(
			rewrite_expr_ordinals(*inner, remap)?,
		))),
		ExecutionExpr::TimeBucket(interval, inner) => Ok(ExecutionExpr::TimeBucket(
			interval,
			Box::new(rewrite_expr_ordinals(*inner, remap)?),
		)),
	}
}

fn bind_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<AggregateCall, BindingError> {
	match function {
		AggregateFunction::CountAll => Ok(AggregateCall {
			function: function.clone(),
			args: Vec::new(),
		}),
		AggregateFunction::Count | AggregateFunction::Sum => {
			let args = args
				.iter()
				.map(|expr| bind_expr(expr, input_schema, config))
				.collect::<Result<Vec<_>, _>>()?;
			if matches!(function, AggregateFunction::Sum) {
				let Some(arg) = args.first() else {
					return Err(BindingError::UnsupportedExpression {
						message: "SUM requires one argument".to_string(),
					});
				};
				if sum_output_data_type(&arg.data_type()).is_none() {
					return Err(BindingError::TypeMismatch {
						message: format!("SUM expects numeric input, got {:?}", arg.data_type()),
					});
				}
			}
			Ok(AggregateCall {
				function: function.clone(),
				args,
			})
		}
		AggregateFunction::LinearRegress => {
			if !config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "LinearRegress",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr(expr, input_schema, config))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(AggregateCall {
				function: function.clone(),
				args,
			})
		}
		AggregateFunction::ApproxQuantile(_) => {
			if !config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "ApproxQuantile",
				});
			}
			let args = args
				.iter()
				.map(|expr| bind_expr(expr, input_schema, config))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(AggregateCall {
				function: function.clone(),
				args,
			})
		}
	}
}

fn bind_expr(
	expr: &Expr,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<ExecutionExpr, BindingError> {
	match expr {
		Expr::Column(name) => Ok(ExecutionExpr::Column(resolve_column(name, input_schema)?)),
		Expr::Literal(value) => Ok(ExecutionExpr::Literal(value.clone())),
		Expr::Eq(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Eq)
		}
		Expr::Neq(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Neq)
		}
		Expr::Lt(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Lt)
		}
		Expr::Lte(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Lte)
		}
		Expr::Gt(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Gt)
		}
		Expr::Gte(left, right) => {
			bind_comparison(input_schema, config, left, right, ExecutionExpr::Gte)
		}
		Expr::And(left, right) => {
			bind_boolean(input_schema, config, left, right, ExecutionExpr::And)
		}
		Expr::Or(left, right) => bind_boolean(input_schema, config, left, right, ExecutionExpr::Or),
		Expr::Not(inner) => {
			let inner = bind_expr(inner, input_schema, config)?;
			require_boolean("NOT", &inner)?;
			Ok(ExecutionExpr::Not(Box::new(inner)))
		}
		Expr::IsNull(inner) => Ok(ExecutionExpr::IsNull(Box::new(bind_expr(
			inner,
			input_schema,
			config,
		)?))),
		Expr::IsNotNull(inner) => Ok(ExecutionExpr::IsNotNull(Box::new(bind_expr(
			inner,
			input_schema,
			config,
		)?))),
		Expr::TimeBucket(interval, inner) => {
			if !config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "TimeBucket",
				});
			}
			let inner = bind_expr(inner, input_schema, config)?;
			if inner.data_type() != DataType::Int64 {
				return Err(BindingError::TypeMismatch {
					message: "TimeBucket expects Int64 (ms) input".to_string(),
				});
			}
			Ok(ExecutionExpr::TimeBucket(*interval, Box::new(inner)))
		}
	}
}

fn bind_comparison(
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	constructor: fn(Box<ExecutionExpr>, Box<ExecutionExpr>) -> ExecutionExpr,
) -> Result<ExecutionExpr, BindingError> {
	let left = bind_expr(left, input_schema, config)?;
	let right = bind_expr(right, input_schema, config)?;
	if left.data_type() != right.data_type() {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"comparison operands have different types: {:?} and {:?}",
				left.data_type(),
				right.data_type()
			),
		});
	}
	Ok(constructor(Box::new(left), Box::new(right)))
}

fn bind_boolean(
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	constructor: fn(Box<ExecutionExpr>, Box<ExecutionExpr>) -> ExecutionExpr,
) -> Result<ExecutionExpr, BindingError> {
	let left = bind_expr(left, input_schema, config)?;
	let right = bind_expr(right, input_schema, config)?;
	require_boolean("boolean operator left", &left)?;
	require_boolean("boolean operator right", &right)?;
	Ok(constructor(Box::new(left), Box::new(right)))
}

fn require_boolean(context: &'static str, expr: &ExecutionExpr) -> Result<(), BindingError> {
	if expr.data_type() == DataType::Boolean {
		Ok(())
	} else {
		Err(BindingError::TypeMismatch {
			message: format!("{context} expected Boolean, got {:?}", expr.data_type()),
		})
	}
}

fn resolve_column(name: &str, input_schema: &SchemaRef) -> Result<ResolvedColumn, BindingError> {
	let ordinal = input_schema
		.index_of(name)
		.map_err(|_| BindingError::UnknownColumn {
			column: name.to_string(),
		})?;
	let field = input_schema.field(ordinal);
	Ok(ResolvedColumn {
		name: ColumnName(field.name().clone()),
		ordinal: ColumnOrdinal(u32::try_from(ordinal).expect("column ordinal overflow")),
		data_type: field.data_type().clone(),
		nullable: field.is_nullable(),
		pbc_eligible: is_pbc_eligible_type(field.data_type()),
	})
}

fn aggregate_schema(group_expr: &[ExecutionExpr], aggr_expr: &[AggregateCall]) -> SchemaRef {
	let mut fields = Vec::new();
	let group_schema = schema_for_exprs(group_expr);
	fields.extend(
		group_schema
			.fields()
			.iter()
			.map(|field| field.as_ref().clone()),
	);
	for call in aggr_expr {
		fields.push(aggregate_field(call));
	}
	Arc::new(Schema::new(fields))
}

fn aggregate_field(call: &AggregateCall) -> Field {
	match call.function {
		AggregateFunction::CountAll => Field::new("count", DataType::Int64, false),
		AggregateFunction::Count => Field::new("count", DataType::Int64, false),
		AggregateFunction::Sum => Field::new(
			"sum",
			call.args
				.first()
				.and_then(|arg| sum_output_data_type(&arg.data_type()))
				.unwrap_or(DataType::Int64),
			true,
		),
		AggregateFunction::LinearRegress => Field::new("linreg", DataType::Float64, true),
		AggregateFunction::ApproxQuantile(_) => Field::new("quantile", DataType::Float64, true),
	}
}

fn sum_output_data_type(input: &DataType) -> Option<DataType> {
	match input {
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64 => Some(DataType::Int64),
		DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
		_ => None,
	}
}

fn is_pbc_eligible_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
			| DataType::LargeUtf8
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::MockCatalog;
	use crate::ir::{QueryProfileHint, ScalarValue, TableId};
	use arrow::datatypes::{Field, Schema};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

	#[tokio::test]
	async fn binder_resolves_columns_to_ordinals() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			exprs: vec![Expr::Column("amount".to_string())],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		match plan {
			ExecutionPlan::Project { input, exprs, .. } => {
				match &exprs[0] {
					ExecutionExpr::Column(column) => {
						assert_eq!(column.ordinal, ColumnOrdinal(0));
						assert_eq!(column.data_type, DataType::Int32);
						assert!(column.nullable);
						assert!(column.pbc_eligible);
					}
					other => panic!("expected resolved column, got {other:?}"),
				}
				match *input {
					ExecutionPlan::Scan {
						projection, schema, ..
					} => {
						assert_eq!(projection, Some(vec![ColumnOrdinal(1)]));
						assert_eq!(schema.field(0).name(), "amount");
					}
					other => panic!("expected scan input, got {other:?}"),
				}
			}
			other => panic!("expected project, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_pushes_exact_projection_union_through_filter() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Filter {
				input: Box::new(LogicalPlan::Scan {
					table: TableId(1),
					projection: None,
				}),
				predicate: Expr::Gt(
					Box::new(Expr::Column("amount".to_string())),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				),
			}),
			exprs: vec![Expr::Column("id".to_string())],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind");
		match plan {
			ExecutionPlan::Project { input, exprs, .. } => {
				match &exprs[0] {
					ExecutionExpr::Column(column) => assert_eq!(column.ordinal, ColumnOrdinal(0)),
					other => panic!("expected project column, got {other:?}"),
				}
				match *input {
					ExecutionPlan::Filter {
						input, predicate, ..
					} => {
						match predicate {
							ExecutionExpr::Gt(left, _) => match *left {
								ExecutionExpr::Column(column) => {
									assert_eq!(column.ordinal, ColumnOrdinal(1))
								}
								other => panic!("expected predicate column, got {other:?}"),
							},
							other => panic!("expected GT predicate, got {other:?}"),
						}
						match *input {
							ExecutionPlan::Scan {
								projection, schema, ..
							} => {
								assert_eq!(
									projection,
									Some(vec![ColumnOrdinal(0), ColumnOrdinal(1)])
								);
								assert_eq!(schema.fields().len(), 2);
								assert_eq!(schema.field(0).name(), "id");
								assert_eq!(schema.field(1).name(), "amount");
							}
							other => panic!("expected scan input, got {other:?}"),
						}
					}
					other => panic!("expected filter input, got {other:?}"),
				}
			}
			other => panic!("expected project, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_rejects_unknown_columns() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			exprs: vec![Expr::Column("missing".to_string())],
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("unknown column should fail");
		assert!(matches!(
			err,
			BindingError::UnknownColumn { column } if column == "missing"
		));
	}

	#[tokio::test]
	async fn binder_gates_experimental_kernels_by_default() {
		let catalog = catalog();
		let plan = LogicalPlan::Project {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			exprs: vec![Expr::TimeBucket(
				std::time::Duration::from_secs(60),
				Box::new(Expr::Column("ts".to_string())),
			)],
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("experimental kernel should fail by default");
		assert!(matches!(
			err,
			BindingError::ExperimentalKernel {
				kernel: "TimeBucket"
			}
		));

		let plan = bind(
			&plan,
			&catalog,
			&PlannerConfig {
				allow_experimental_kernels: true,
			},
		)
		.await
		.expect("experimental kernel enabled");
		assert!(matches!(plan, ExecutionPlan::Project { .. }));
	}

	#[tokio::test]
	async fn binder_shapes_grouped_aggregate_plan() {
		let catalog = catalog();
		let plan = LogicalPlan::Aggregate {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			group_expr: vec![Expr::Column("region".to_string())],
			aggr_expr: vec![(
				AggregateFunction::Sum,
				vec![Expr::Column("amount".to_string())],
			)],
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind grouped aggregate");
		match plan {
			ExecutionPlan::Aggregate { output_schema, .. } => {
				assert_eq!(output_schema.field(0).name(), "region");
				assert_eq!(output_schema.field(1).name(), "sum");
			}
			other => panic!("expected aggregate, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_attaches_pbc_program_for_eligible_scan_filter() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Gt(
				Box::new(Expr::Column("amount".to_string())),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind eligible filter");
		match plan {
			ExecutionPlan::Filter {
				input, disposition, ..
			} => {
				assert_eq!(disposition, FilterDisposition::FullyPushed);
				match *input {
					ExecutionPlan::Scan { pbc_program, .. } => assert!(pbc_program.is_some()),
					other => panic!("expected scan input, got {other:?}"),
				}
			}
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_selects_point_read_profile_for_primary_key_equality() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::Column("id".to_string())),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(7)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind primary-key equality");

		match plan {
			ExecutionPlan::Filter { input, .. } => match *input {
				ExecutionPlan::Scan {
					profile_hint,
					primary_key_columns,
					..
				} => {
					assert_eq!(profile_hint, QueryProfileHint::PointRead);
					assert_eq!(primary_key_columns, vec![ColumnOrdinal(0)]);
				}
				other => panic!("expected scan input, got {other:?}"),
			},
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_keeps_scan_profile_for_non_primary_key_equality() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::Column("amount".to_string())),
				Box::new(Expr::Literal(ScalarValue::Int32(Some(7)))),
			),
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind non-primary-key equality");

		match plan {
			ExecutionPlan::Filter { input, .. } => match *input {
				ExecutionPlan::Scan { profile_hint, .. } => {
					assert_eq!(profile_hint, QueryProfileHint::Scan);
				}
				other => panic!("expected scan input, got {other:?}"),
			},
			other => panic!("expected filter, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn binder_marks_ineligible_filters_for_toron_evaluation() {
		let catalog = catalog();
		let plan = LogicalPlan::Filter {
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
			predicate: Expr::Eq(
				Box::new(Expr::Literal(ScalarValue::Int32(Some(1)))),
				Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
			),
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("type mismatch should fail before pushdown");
		assert!(matches!(err, BindingError::TypeMismatch { .. }));
	}

	fn catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor())
			.expect("register descriptor");
		catalog
	}

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
			Field::new("ts", DataType::Int64, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([1; 16]),
			1,
			"default",
			"sales",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
