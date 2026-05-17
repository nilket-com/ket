use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::aggregate_kernel::{
	self, AggregateArity, AggregateInputDomain, AggregateKernelSpec, StableAggregateKernel,
};
use crate::catalog::{Catalog, TableBinding, TableRef};
use crate::error::BindingError;
use crate::ir::{
	AggregateCall, AggregateFunction, ColumnName, ColumnOrdinal, ExecutionExpr, ExecutionPlan,
	ExecutionSortKey, Expr, FilterDisposition, JoinKind, JoinStrategy, LogicalPlan, PbcProgram,
	QueryProfileHint, ResolvedColumn, ScalarValue, SortExpr, arithmetic_output_type,
	schema_for_exprs,
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
				LogicalPlan::Alias { input, .. } => self.bind_plan(input).await,
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
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					if exprs.len() != output_names.len() {
						return Err(BindingError::UnsupportedExpression {
							message: "internal: Project exprs/output_names length mismatch"
								.to_string(),
						});
					}
					let input = self.bind_plan(input).await?;
					let exprs = exprs
						.iter()
						.map(|expr| bind_expr(expr, &input.schema(), self.config))
						.collect::<Result<Vec<_>, _>>()?;
					let output_schema = schema_for_exprs_with_output_names(&exprs, output_names);

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
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => {
					let input = self.bind_plan(input).await?;
					let output_schema = input.schema();
					let order_by = bind_sort_keys(order_by, &output_schema)?;

					Ok(ExecutionPlan::Sort {
						input: Box::new(input),
						order_by,
						limit: *limit,
						offset: *offset,
						output_schema,
					})
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					strategy,
				} => self.bind_join(left, right, *kind, on, *strategy).await,
			}
		})
	}

	async fn bind_join(
		&self,
		left: &LogicalPlan,
		right: &LogicalPlan,
		kind: JoinKind,
		on: &[(Expr, Expr)],
		strategy: JoinStrategy,
	) -> Result<ExecutionPlan, BindingError> {
		let left_plan = self.bind_plan(left).await?;
		let right_plan = self.bind_plan(right).await?;
		let left_scope = Scope::from_plan(left, &left_plan.schema());
		let right_scope = Scope::from_plan(right, &right_plan.schema());
		let mut left_keys = Vec::new();
		let mut right_keys = Vec::new();

		for (left_expr, right_expr) in on {
			let (left_key, right_key) =
				bind_join_key_pair(left_expr, right_expr, &left_scope, &right_scope)?;
			validate_join_key_types(&left_key, &right_key)?;
			left_keys.push(left_key.ordinal);
			right_keys.push(right_key.ordinal);
		}

		validate_strategy_shape(
			kind,
			strategy,
			&left_plan,
			&right_plan,
			&left_keys,
			&right_keys,
		)?;

		Ok(ExecutionPlan::Join {
			left: Box::new(left_plan),
			right: Box::new(right_plan),
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema: join_schema(kind, &left_scope.schema, &right_scope.schema),
		})
	}
}

fn bind_sort_keys(
	order_by: &[SortExpr],
	output_schema: &SchemaRef,
) -> Result<Vec<ExecutionSortKey>, BindingError> {
	if order_by.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "ORDER BY requires at least one key".to_string(),
		});
	}

	let mut keys = Vec::with_capacity(order_by.len() + output_schema.fields().len());
	let mut user_ordinals = BTreeSet::new();
	for key in order_by {
		let ordinal = bind_sort_key_ordinal(&key.expr, output_schema)?;
		user_ordinals.insert(ordinal);
		keys.push(ExecutionSortKey {
			ordinal,
			descending: key.descending,
			nulls_first: key.nulls_first,
		});
	}

	for ordinal in output_columns(output_schema.fields().len()) {
		if !user_ordinals.contains(&ordinal) {
			keys.push(ExecutionSortKey {
				ordinal,
				descending: false,
				nulls_first: false,
			});
		}
	}

	Ok(keys)
}

fn bind_sort_key_ordinal(
	expr: &Expr,
	output_schema: &SchemaRef,
) -> Result<ColumnOrdinal, BindingError> {
	match expr {
		Expr::Column(column_ref) => {
			if column_ref.qualifier.is_some() {
				return Err(BindingError::UnsupportedExpression {
					message: "ORDER BY only supports unqualified output column names".to_string(),
				});
			}
			Ok(resolve_column(&column_ref.name.0, output_schema)?.ordinal)
		}
		Expr::Literal(ScalarValue::Int32(Some(value))) => {
			bind_sort_ordinal_literal(i64::from(*value), output_schema)
		}
		Expr::Literal(ScalarValue::Int64(Some(value))) => {
			bind_sort_ordinal_literal(*value, output_schema)
		}
		Expr::Literal(_) => Err(BindingError::UnsupportedExpression {
			message: "ORDER BY ordinals must be positive integers".to_string(),
		}),
		_ => Err(BindingError::UnsupportedExpression {
			message: "ORDER BY keys must reference output columns or positive ordinals".to_string(),
		}),
	}
}

fn bind_sort_ordinal_literal(
	value: i64,
	output_schema: &SchemaRef,
) -> Result<ColumnOrdinal, BindingError> {
	if value <= 0 {
		return Err(BindingError::UnsupportedExpression {
			message: "ORDER BY ordinal must be positive".to_string(),
		});
	}
	let zero_based =
		usize::try_from(value - 1).map_err(|_| BindingError::UnsupportedExpression {
			message: "ORDER BY ordinal is out of range".to_string(),
		})?;
	if zero_based >= output_schema.fields().len() {
		return Err(BindingError::UnknownColumn {
			column: format!("ORDER BY ordinal {value}"),
		});
	}
	Ok(ColumnOrdinal(
		u32::try_from(zero_based).expect("column ordinal overflow"),
	))
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

#[derive(Debug, Clone)]
struct Scope {
	schema: SchemaRef,
	qualifiers: Vec<TableRef>,
}

impl Scope {
	fn from_plan(plan: &LogicalPlan, schema: &SchemaRef) -> Self {
		let mut qualifiers = Vec::new();
		collect_qualifiers(plan, &mut qualifiers);
		Self {
			schema: schema.clone(),
			qualifiers,
		}
	}

	fn resolve_join_column(
		&self,
		column_ref: &crate::ir::ColumnRef,
		side: &'static str,
	) -> Result<Option<ResolvedColumn>, BindingError> {
		if let Some(qualifier) = &column_ref.qualifier {
			if !self
				.qualifiers
				.iter()
				.any(|candidate| candidate == qualifier)
			{
				return Ok(None);
			}
			return resolve_column(&column_ref.name.0, &self.schema).map(Some);
		}

		match resolve_column(&column_ref.name.0, &self.schema) {
			Ok(column) => Ok(Some(column)),
			Err(BindingError::UnknownColumn { .. }) => Ok(None),
			Err(error) => Err(error),
		}
		.map_err(|error| match error {
			BindingError::AmbiguousColumn { column } => BindingError::AmbiguousColumnRef {
				name: column,
				found_in: vec![side.to_string()],
			},
			other => other,
		})
	}
}

fn collect_qualifiers(plan: &LogicalPlan, qualifiers: &mut Vec<TableRef>) {
	match plan {
		LogicalPlan::Scan { .. } => {}
		LogicalPlan::ScanNamed { table, .. } => qualifiers.push(table.clone()),
		LogicalPlan::Alias { input, alias } => {
			collect_qualifiers(input, qualifiers);
			qualifiers.push(alias.clone());
		}
		LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. } => collect_qualifiers(input, qualifiers),
		LogicalPlan::Join {
			left, right, kind, ..
		} => match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => collect_qualifiers(left, qualifiers),
			JoinKind::RightSemi | JoinKind::RightAnti => collect_qualifiers(right, qualifiers),
			JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
				collect_qualifiers(left, qualifiers);
				collect_qualifiers(right, qualifiers);
			}
		},
	}
}

fn bind_join_key_pair(
	left_expr: &Expr,
	right_expr: &Expr,
	left_scope: &Scope,
	right_scope: &Scope,
) -> Result<(ResolvedColumn, ResolvedColumn), BindingError> {
	let (left_ref, right_ref) = match (left_expr, right_expr) {
		(Expr::Column(left_ref), Expr::Column(right_ref)) => (left_ref, right_ref),
		(Expr::Eq(_, _), _) | (_, Expr::Eq(_, _)) => {
			return Err(BindingError::JoinPredicateNotEqui);
		}
		_ => return Err(BindingError::JoinComputedKeyUnsupported),
	};

	match (
		resolve_join_column_across(left_ref, left_scope, right_scope)?,
		resolve_join_column_across(right_ref, left_scope, right_scope)?,
	) {
		(JoinColumnResolution::Left(left), JoinColumnResolution::Right(right)) => Ok((left, right)),
		(JoinColumnResolution::Right(right), JoinColumnResolution::Left(left)) => Ok((left, right)),
		(JoinColumnResolution::Left(_), JoinColumnResolution::Left(_))
		| (JoinColumnResolution::Right(_), JoinColumnResolution::Right(_)) => {
			Err(BindingError::JoinComputedKeyUnsupported)
		}
	}
}

enum JoinColumnResolution {
	Left(ResolvedColumn),
	Right(ResolvedColumn),
}

fn resolve_join_column_across(
	column_ref: &crate::ir::ColumnRef,
	left_scope: &Scope,
	right_scope: &Scope,
) -> Result<JoinColumnResolution, BindingError> {
	let left = left_scope.resolve_join_column(column_ref, "left")?;
	let right = right_scope.resolve_join_column(column_ref, "right")?;
	match (left, right) {
		(Some(left), None) => Ok(JoinColumnResolution::Left(left)),
		(None, Some(right)) => Ok(JoinColumnResolution::Right(right)),
		(Some(_), Some(_)) => Err(BindingError::AmbiguousColumnRef {
			name: column_ref.name.0.clone(),
			found_in: vec!["left".to_string(), "right".to_string()],
		}),
		(None, None) => {
			if let Some(qualifier) = &column_ref.qualifier {
				Err(BindingError::UnknownTableRef {
					qualifier: table_name(qualifier),
				})
			} else {
				Err(BindingError::UnknownColumn {
					column: column_ref.name.0.clone(),
				})
			}
		}
	}
}

fn validate_join_key_types(
	left: &ResolvedColumn,
	right: &ResolvedColumn,
) -> Result<(), BindingError> {
	if left.data_type != right.data_type {
		return Err(BindingError::JoinKeyTypeMismatch {
			left_type: format!("{:?}", left.data_type),
			right_type: format!("{:?}", right.data_type),
		});
	}
	if matches!(
		left.data_type,
		DataType::Float16 | DataType::Float32 | DataType::Float64
	) {
		return Err(BindingError::JoinKeyTypeUnsupported {
			type_name: format!("{:?}", left.data_type),
		});
	}
	Ok(())
}

fn validate_strategy_shape(
	kind: JoinKind,
	strategy: JoinStrategy,
	left: &ExecutionPlan,
	right: &ExecutionPlan,
	left_keys: &[ColumnOrdinal],
	right_keys: &[ColumnOrdinal],
) -> Result<(), BindingError> {
	match strategy {
		JoinStrategy::HashPartitioned => {
			let Some((left_count, left_exchange_keys)) = terminal_exchange(left) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			let Some((right_count, right_exchange_keys)) = terminal_exchange(right) else {
				return Err(BindingError::JoinKeyExchangeMismatch);
			};
			if left_count != right_count {
				return Err(BindingError::JoinPartitionCountMismatch {
					left: left_count,
					right: right_count,
				});
			}
			if exchange_key_ordinals(left_exchange_keys) != left_keys
				|| exchange_key_ordinals(right_exchange_keys) != right_keys
			{
				return Err(BindingError::JoinKeyExchangeMismatch);
			}
		}
		JoinStrategy::Broadcast => {
			if kind != JoinKind::Inner {
				return Err(BindingError::UnsupportedExpression {
					message: "broadcast join is only supported for INNER joins".to_string(),
				});
			}
			let Some((left_count, _)) = terminal_exchange(left) else {
				return Err(BindingError::BroadcastBuildSidePlacement);
			};
			if left_count != 1 {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
			if terminal_exchange(right).is_some() {
				return Err(BindingError::BroadcastBuildSidePlacement);
			}
		}
	}
	Ok(())
}

fn terminal_exchange(plan: &ExecutionPlan) -> Option<(u32, &[ExecutionExpr])> {
	match plan {
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} => Some((*partition_count, key.as_slice())),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. } => terminal_exchange(input),
		ExecutionPlan::Scan { .. } | ExecutionPlan::Join { .. } => None,
	}
}

fn exchange_key_ordinals(key: &[ExecutionExpr]) -> Vec<ColumnOrdinal> {
	key.iter()
		.filter_map(|expr| match expr {
			ExecutionExpr::Column(column) => Some(column.ordinal),
			_ => None,
		})
		.collect()
}

pub fn join_schema(kind: JoinKind, left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
	let fields = match kind {
		JoinKind::Inner => join_fields(left, right),
		JoinKind::LeftOuter => left_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::RightOuter => nullable_fields(left)
			.into_iter()
			.chain(left_fields(right))
			.collect(),
		JoinKind::FullOuter => nullable_fields(left)
			.into_iter()
			.chain(nullable_fields(right))
			.collect(),
		JoinKind::LeftSemi | JoinKind::LeftAnti => left_fields(left),
		JoinKind::RightSemi | JoinKind::RightAnti => left_fields(right),
	};
	Arc::new(Schema::new(fields))
}

fn join_fields(left: &SchemaRef, right: &SchemaRef) -> Vec<Field> {
	left_fields(left)
		.into_iter()
		.chain(left_fields(right))
		.collect()
}

fn left_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone())
		.collect()
}

fn nullable_fields(schema: &SchemaRef) -> Vec<Field> {
	schema
		.fields()
		.iter()
		.map(|field| field.as_ref().clone().with_nullable(true))
		.collect()
}

fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
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
			output_schema,
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
			let output_schema = schema_for_exprs_with_existing_names(&exprs, &output_schema);
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
		ExecutionPlan::Sort {
			input,
			order_by,
			limit,
			offset,
			output_schema,
		} => {
			let output_len = output_schema.fields().len();
			let (input, _) = optimize_for_required(*input, None)?;
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);

			Ok((
				ExecutionPlan::Sort {
					input: Box::new(input),
					order_by,
					limit,
					offset,
					output_schema,
				},
				output_remap,
			))
		}
		ExecutionPlan::Join {
			left,
			right,
			kind,
			left_keys,
			right_keys,
			strategy,
			output_schema,
		} => {
			let (left, _) = optimize_for_required(*left, None)?;
			let (right, _) = optimize_for_required(*right, None)?;
			let output_len = output_schema.fields().len();
			let output_remap = ordinal_remap(
				&output_columns(output_len)
					.iter()
					.copied()
					.collect::<Vec<_>>(),
			);
			Ok((
				ExecutionPlan::Join {
					left: Box::new(left),
					right: Box::new(right),
					kind,
					left_keys,
					right_keys,
					strategy,
					output_schema,
				},
				output_remap,
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
		ExecutionExpr::Cast(inner, _) | ExecutionExpr::TryCast(inner, _) => {
			collect_expr_columns(inner, columns)
		}
		ExecutionExpr::Add(left, right)
		| ExecutionExpr::Sub(left, right)
		| ExecutionExpr::Mul(left, right)
		| ExecutionExpr::Div(left, right)
		| ExecutionExpr::Eq(left, right)
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
		ExecutionExpr::Cast(inner, target) => Ok(ExecutionExpr::Cast(
			Box::new(rewrite_expr_ordinals(*inner, remap)?),
			target,
		)),
		ExecutionExpr::TryCast(inner, target) => Ok(ExecutionExpr::TryCast(
			Box::new(rewrite_expr_ordinals(*inner, remap)?),
			target,
		)),
		ExecutionExpr::Add(left, right) => Ok(ExecutionExpr::Add(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Sub(left, right) => Ok(ExecutionExpr::Sub(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Mul(left, right) => Ok(ExecutionExpr::Mul(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
		ExecutionExpr::Div(left, right) => Ok(ExecutionExpr::Div(
			Box::new(rewrite_expr_ordinals(*left, remap)?),
			Box::new(rewrite_expr_ordinals(*right, remap)?),
		)),
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
	if matches!(function, AggregateFunction::CountAll) {
		return Ok(AggregateCall {
			function: function.clone(),
			args: Vec::new(),
		});
	}
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return bind_stable_aggregate_call(function, args, input_schema, config, spec);
	}
	match function {
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
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

fn bind_stable_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	spec: &AggregateKernelSpec,
) -> Result<AggregateCall, BindingError> {
	if args.len() != aggregate_arity_len(spec.arity) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("{} requires one argument", spec.sql_name),
		});
	}
	let args = args
		.iter()
		.map(|expr| bind_expr(expr, input_schema, config))
		.collect::<Result<Vec<_>, _>>()?;
	if !matches!(spec.input_domain, AggregateInputDomain::Any) {
		let Some(arg) = args.first() else {
			return Err(BindingError::UnsupportedExpression {
				message: format!("{} requires one argument", spec.sql_name),
			});
		};
		let data_type = arg.data_type();
		if aggregate_kernel::stable_output_data_type(function, Some(&data_type)).is_none() {
			return Err(BindingError::TypeMismatch {
				message: aggregate_type_mismatch_message(spec.kind, &data_type),
			});
		}
	}
	Ok(AggregateCall {
		function: function.clone(),
		args,
	})
}

fn aggregate_arity_len(arity: AggregateArity) -> usize {
	match arity {
		AggregateArity::Zero => 0,
		AggregateArity::One => 1,
	}
}

fn aggregate_type_mismatch_message(kernel: StableAggregateKernel, data_type: &DataType) -> String {
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

fn bind_expr(
	expr: &Expr,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<ExecutionExpr, BindingError> {
	match expr {
		Expr::Column(column_ref) => Ok(ExecutionExpr::Column(resolve_column(
			&column_ref.name.0,
			input_schema,
		)?)),
		Expr::Literal(value) => Ok(ExecutionExpr::Literal(value.clone())),
		Expr::Cast(inner, target) => bind_explicit_cast(input_schema, config, inner, target),
		Expr::TryCast(inner, target) => bind_try_cast(input_schema, config, inner, target),
		Expr::Add(left, right) => {
			bind_arithmetic(input_schema, config, left, right, ExecutionExpr::Add)
		}
		Expr::Sub(left, right) => {
			bind_arithmetic(input_schema, config, left, right, ExecutionExpr::Sub)
		}
		Expr::Mul(left, right) => {
			bind_arithmetic(input_schema, config, left, right, ExecutionExpr::Mul)
		}
		Expr::Div(left, right) => {
			bind_arithmetic(input_schema, config, left, right, ExecutionExpr::Div)
		}
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

fn bind_arithmetic(
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	constructor: fn(Box<ExecutionExpr>, Box<ExecutionExpr>) -> ExecutionExpr,
) -> Result<ExecutionExpr, BindingError> {
	let left = bind_expr(left, input_schema, config)?;
	let right = bind_expr(right, input_schema, config)?;
	let left_type = left.data_type();
	let right_type = right.data_type();
	let output_type =
		arithmetic_output_type(&left_type, &right_type).ok_or_else(|| {
			BindingError::TypeMismatch {
				message: format!(
					"arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and {right_type:?}"
				),
			}
		})?;
	let left = cast_if_needed(left, &output_type)?;
	let right = cast_if_needed(right, &output_type)?;
	Ok(constructor(Box::new(left), Box::new(right)))
}

fn bind_explicit_cast(
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	inner: &Expr,
	target: &DataType,
) -> Result<ExecutionExpr, BindingError> {
	let inner = bind_expr(inner, input_schema, config)?;
	let source = inner.data_type();
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(ExecutionExpr::Cast(Box::new(inner), target.clone()))
}

fn bind_try_cast(
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	inner: &Expr,
	target: &DataType,
) -> Result<ExecutionExpr, BindingError> {
	let inner = bind_expr(inner, input_schema, config)?;
	let source = inner.data_type();
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(ExecutionExpr::TryCast(Box::new(inner), target.clone()))
}

fn cast_if_needed(expr: ExecutionExpr, target: &DataType) -> Result<ExecutionExpr, BindingError> {
	let source = expr.data_type();
	if source == *target {
		return Ok(expr);
	}
	if !is_widening_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"Cast from {source:?} to {target:?} is not a supported widening conversion"
			),
		});
	}
	Ok(ExecutionExpr::Cast(Box::new(expr), target.clone()))
}

fn is_widening_cast(source: &DataType, target: &DataType) -> bool {
	matches!(
		(source, target),
		(DataType::Int32, DataType::Int64)
			| (DataType::Int32, DataType::Float64)
			| (DataType::Int64, DataType::Float64)
			| (DataType::Float32, DataType::Float64)
	)
}

fn is_supported_explicit_numeric_cast(source: &DataType, target: &DataType) -> bool {
	matches!(
		(source, target),
		(DataType::Int32, DataType::Int32)
			| (DataType::Int32, DataType::Int64)
			| (DataType::Int32, DataType::Float32)
			| (DataType::Int32, DataType::Float64)
			| (DataType::Int64, DataType::Int32)
			| (DataType::Int64, DataType::Int64)
			| (DataType::Int64, DataType::Float32)
			| (DataType::Int64, DataType::Float64)
			| (DataType::Float32, DataType::Int32)
			| (DataType::Float32, DataType::Int64)
			| (DataType::Float32, DataType::Float32)
			| (DataType::Float32, DataType::Float64)
			| (DataType::Float64, DataType::Int32)
			| (DataType::Float64, DataType::Int64)
			| (DataType::Float64, DataType::Float32)
			| (DataType::Float64, DataType::Float64)
	)
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
	let (left, right) = coerce_comparison_literals(left, right);
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

fn coerce_comparison_literals(
	left: ExecutionExpr,
	right: ExecutionExpr,
) -> (ExecutionExpr, ExecutionExpr) {
	match (left, right) {
		(left @ ExecutionExpr::Literal(_), right @ ExecutionExpr::Literal(_)) => (left, right),
		(ExecutionExpr::Literal(value), right) => {
			let data_type = right.data_type();
			(
				ExecutionExpr::Literal(coerce_literal_value(value, &data_type)),
				right,
			)
		}
		(left, ExecutionExpr::Literal(value)) => {
			let data_type = left.data_type();
			(
				left,
				ExecutionExpr::Literal(coerce_literal_value(value, &data_type)),
			)
		}
		(left, right) => (left, right),
	}
}

fn coerce_literal_value(value: ScalarValue, target_type: &DataType) -> ScalarValue {
	match (value, target_type) {
		(ScalarValue::Int32(value), DataType::Int64) => ScalarValue::Int64(value.map(i64::from)),
		(ScalarValue::Int32(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(ScalarValue::Int32(value), DataType::Float64) => {
			ScalarValue::Float64(value.map(f64::from))
		}
		(ScalarValue::Int64(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(ScalarValue::Int64(value), DataType::Float64) => {
			ScalarValue::Float64(value.map(|value| value as f64))
		}
		(ScalarValue::Float64(value), DataType::Float32) => {
			ScalarValue::Float32(value.map(|value| value as f32))
		}
		(value, _) => value,
	}
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
	let matches = input_schema
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| field.name() == name)
		.map(|(idx, _)| idx)
		.collect::<Vec<_>>();
	let ordinal = match matches.as_slice() {
		[] => {
			return Err(BindingError::UnknownColumn {
				column: name.to_string(),
			});
		}
		[ordinal] => *ordinal,
		_ => {
			return Err(BindingError::AmbiguousColumn {
				column: name.to_string(),
			});
		}
	};
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

fn schema_for_exprs_with_output_names(
	exprs: &[ExecutionExpr],
	output_names: &[Option<String>],
) -> SchemaRef {
	debug_assert_eq!(exprs.len(), output_names.len());
	let derived = schema_for_exprs(exprs);
	let fields = exprs
		.iter()
		.enumerate()
		.map(|(idx, expr)| {
			let name = output_names[idx]
				.as_ref()
				.cloned()
				.unwrap_or_else(|| derived.field(idx).name().clone());
			Field::new(name, expr.data_type(), expr.nullable())
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn schema_for_exprs_with_existing_names(
	exprs: &[ExecutionExpr],
	existing_schema: &SchemaRef,
) -> SchemaRef {
	let fields = exprs
		.iter()
		.enumerate()
		.map(|(idx, expr)| {
			let name = existing_schema
				.fields()
				.get(idx)
				.map(|field| field.name().clone())
				.unwrap_or_else(|| match expr {
					ExecutionExpr::Column(column) => column.name.0.clone(),
					_ => format!("col_{idx}"),
				});
			Field::new(name, expr.data_type(), expr.nullable())
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn aggregate_field(call: &AggregateCall) -> Field {
	if let Some(field) = aggregate_kernel::stable_output_field(call) {
		return field;
	}
	match call.function {
		AggregateFunction::LinearRegress => Field::new("linreg", DataType::Float64, true),
		AggregateFunction::ApproxQuantile(_) => Field::new("quantile", DataType::Float64, true),
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
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
			output_names: vec![None],
			exprs: vec![Expr::column("amount")],
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
					Box::new(Expr::column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				),
			}),
			output_names: vec![None],
			exprs: vec![Expr::column("id")],
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
			output_names: vec![None],
			exprs: vec![Expr::column("missing")],
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
			output_names: vec![None],
			exprs: vec![Expr::TimeBucket(
				std::time::Duration::from_secs(60),
				Box::new(Expr::column("ts")),
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
			group_expr: vec![Expr::column("region")],
			aggr_expr: vec![(AggregateFunction::Sum, vec![Expr::column("amount")])],
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
				Box::new(Expr::column("amount")),
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
				Box::new(Expr::column("id")),
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
				Box::new(Expr::column("amount")),
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

	#[tokio::test]
	async fn bind_hash_join_resolves_qualified_keys_and_duplicate_output_names() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new("orders"),
					projection: None,
				}),
				partition_count: 2,
				key: vec![Expr::qualified_column(
					TableRef::new("orders"),
					"customer_id",
				)],
			}),
			right: Box::new(LogicalPlan::Exchange {
				input: Box::new(LogicalPlan::ScanNamed {
					table: TableRef::new("customers"),
					projection: None,
				}),
				partition_count: 2,
				key: vec![Expr::qualified_column(TableRef::new("customers"), "id")],
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind join");

		match plan {
			ExecutionPlan::Join {
				left_keys,
				right_keys,
				output_schema,
				..
			} => {
				assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
				assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
				assert_eq!(output_schema.fields().len(), 4);
				assert_eq!(output_schema.field(0).name(), "id");
				assert_eq!(output_schema.field(2).name(), "id");
			}
			other => panic!("expected join, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bind_hash_join_auto_swaps_reversed_on_pair() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		let plan = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind join");

		match plan {
			ExecutionPlan::Join {
				left_keys,
				right_keys,
				..
			} => {
				assert_eq!(left_keys, vec![ColumnOrdinal(1)]);
				assert_eq!(right_keys, vec![ColumnOrdinal(0)]);
			}
			other => panic!("expected join, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bind_join_rejects_ambiguous_unqualified_column() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(Expr::column("id"), Expr::column("id"))],
			strategy: JoinStrategy::HashPartitioned,
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("ambiguous join key should fail");

		assert!(matches!(err, BindingError::AmbiguousColumnRef { name, .. } if name == "id"));
	}

	#[tokio::test]
	async fn bind_join_rejects_unknown_qualifier() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("missing"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		let err = bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect_err("unknown qualifier should fail");

		assert!(matches!(
			err,
			BindingError::UnknownTableRef { qualifier } if qualifier == "missing"
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_non_equi_and_computed_keys() {
		let catalog = join_catalog();
		let non_equi = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::Eq(
					Box::new(Expr::qualified_column(
						TableRef::new("orders"),
						"customer_id",
					)),
					Box::new(Expr::qualified_column(TableRef::new("customers"), "id")),
				),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};
		let computed = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::Literal(ScalarValue::Int64(Some(7))),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		assert!(matches!(
			bind(&non_equi, &catalog, &PlannerConfig::default())
				.await
				.expect_err("non-equi should fail"),
			BindingError::JoinPredicateNotEqui
		));
		assert!(matches!(
			bind(&computed, &catalog, &PlannerConfig::default())
				.await
				.expect_err("computed key should fail"),
			BindingError::JoinComputedKeyUnsupported
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_key_type_mismatch_and_float_keys() {
		let catalog = join_catalog();
		let mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "region", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "region"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		assert!(matches!(
			bind(&mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("type mismatch should fail"),
			BindingError::JoinKeyTypeMismatch { .. }
		));

		let catalog = float_join_catalog();
		let float_key = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("left_floats", "f", 2)),
			right: Box::new(exchanged_named_scan("right_floats", "f", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("left_floats"), "f"),
				Expr::qualified_column(TableRef::new("right_floats"), "f"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		assert!(matches!(
			bind(&float_key, &catalog, &PlannerConfig::default())
				.await
				.expect_err("float key should fail"),
			BindingError::JoinKeyTypeUnsupported { .. }
		));
	}

	#[tokio::test]
	async fn bind_join_rejects_exchange_partition_and_key_shape_mismatches() {
		let catalog = join_catalog();
		let partition_mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "customer_id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 3)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};
		let key_mismatch = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("orders", "id", 2)),
			right: Box::new(exchanged_named_scan("customers", "id", 2)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::HashPartitioned,
		};

		assert!(matches!(
			bind(&partition_mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("partition mismatch should fail"),
			BindingError::JoinPartitionCountMismatch { left: 2, right: 3 }
		));
		assert!(matches!(
			bind(&key_mismatch, &catalog, &PlannerConfig::default())
				.await
				.expect_err("exchange key mismatch should fail"),
			BindingError::JoinKeyExchangeMismatch
		));
	}

	#[tokio::test]
	async fn bind_broadcast_join_enforces_left_single_partition_build() {
		let catalog = join_catalog();
		let plan = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 1)),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::Broadcast,
		};

		assert!(matches!(
			bind(&plan, &catalog, &PlannerConfig::default())
				.await
				.expect("bind broadcast"),
			ExecutionPlan::Join {
				strategy: JoinStrategy::Broadcast,
				..
			}
		));

		let wrong_side = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			right: Box::new(exchanged_named_scan("customers", "id", 1)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
				Expr::qualified_column(TableRef::new("customers"), "id"),
			)],
			strategy: JoinStrategy::Broadcast,
		};

		assert!(matches!(
			bind(&wrong_side, &catalog, &PlannerConfig::default())
				.await
				.expect_err("right-side publish must fail"),
			BindingError::BroadcastBuildSidePlacement
		));
	}

	#[tokio::test]
	async fn bind_broadcast_join_rejects_all_malformed_publish_shapes() {
		let catalog = join_catalog();
		let missing_left_publish = LogicalPlan::Join {
			left: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("customers"),
				projection: None,
			}),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::Broadcast,
		};
		let wrong_left_partition_count = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 4)),
			right: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new("orders"),
				projection: None,
			}),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::Broadcast,
		};
		let both_sides_published = LogicalPlan::Join {
			left: Box::new(exchanged_named_scan("customers", "id", 1)),
			right: Box::new(exchanged_named_scan("orders", "customer_id", 1)),
			kind: JoinKind::Inner,
			on: vec![(
				Expr::qualified_column(TableRef::new("customers"), "id"),
				Expr::qualified_column(TableRef::new("orders"), "customer_id"),
			)],
			strategy: JoinStrategy::Broadcast,
		};

		for plan in [
			missing_left_publish,
			wrong_left_partition_count,
			both_sides_published,
		] {
			assert!(matches!(
				bind(&plan, &catalog, &PlannerConfig::default())
					.await
					.expect_err("malformed broadcast shape must fail"),
				BindingError::BroadcastBuildSidePlacement
			));
		}
	}

	fn catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(1), descriptor())
			.expect("register descriptor");
		catalog
	}

	fn join_catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(1),
				named_descriptor(
					1,
					"orders",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("customer_id", DataType::Int64, false),
					]),
				),
			)
			.expect("register orders");
		catalog
			.register_descriptor(
				TableId(2),
				named_descriptor(
					2,
					"customers",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("region", DataType::Utf8, true),
					]),
				),
			)
			.expect("register customers");
		catalog
	}

	fn float_join_catalog() -> MockCatalog {
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(
				TableId(11),
				named_descriptor(
					11,
					"left_floats",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("f", DataType::Float64, false),
					]),
				),
			)
			.expect("register left floats");
		catalog
			.register_descriptor(
				TableId(12),
				named_descriptor(
					12,
					"right_floats",
					Schema::new(vec![
						Field::new("id", DataType::Int64, false),
						Field::new("f", DataType::Float64, false),
					]),
				),
			)
			.expect("register right floats");
		catalog
	}

	fn exchanged_named_scan(table: &str, key: &str, partition_count: u32) -> LogicalPlan {
		LogicalPlan::Exchange {
			input: Box::new(LogicalPlan::ScanNamed {
				table: TableRef::new(table),
				projection: None,
			}),
			partition_count,
			key: vec![Expr::qualified_column(TableRef::new(table), key)],
		}
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

	fn named_descriptor(id_byte: u8, table: &str, schema: Schema) -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([id_byte; 16]),
			1,
			"default",
			table,
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
