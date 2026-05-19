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
	AggregateFunction, BoundAggregateCall, BoundBinaryOp, BoundExprId, BoundExprNode,
	BoundExprProgram, BoundExprProgramBuilder, BoundUnaryOp, ColumnName, ColumnOrdinal,
	ExecutionPlan, ExecutionSortKey, Expr, FilterDisposition, InsertLiteral, JoinKind,
	JoinStrategy, LogicalPlan, PbcProgram, QueryProfileHint, ResolvedColumn, ScalarValue, SortExpr,
	arithmetic_output_type, schema_for_exprs,
};
use crate::pbc::PbcCompiler;
use wendelon::schema::{
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN,
};

pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
const STORAGE_SYNTHETIC_COLUMN_NAMES: &[&str] = &[
	STORAGE_ROW_HLC_COLUMN,
	STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN,
];

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
					let bound_predicate = bind_expr_program(
						std::slice::from_ref(predicate),
						&input.schema(),
						self.config,
					)?;
					let profile_hint = scan_profile_hint(&bound_predicate, &input);
					apply_scan_profile_hint(&mut input, profile_hint);
					let (input, disposition) = match compile_pbc(&bound_predicate) {
						Some(program) => {
							let (input, attached) = attach_pbc_program(input, program);
							let disposition = if attached {
								FilterDisposition::FullyPushed
							} else {
								FilterDisposition::EvaluateInToron
							};
							(input, disposition)
						}
						None => (input, FilterDisposition::EvaluateInToron),
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
					let exprs = bind_expr_program(exprs, &input.schema(), self.config)?;
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
					let group_expr = bind_expr_program(group_expr, &input_schema, self.config)?;
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
					let key = bind_expr_program(key, &input_schema, self.config)?;

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
				LogicalPlan::CreateTable {
					name,
					schema,
					if_not_exists,
					or_replace,
				} => Ok(ExecutionPlan::CreateTable {
					name: name.clone(),
					schema: schema.clone(),
					if_not_exists: *if_not_exists,
					or_replace: *or_replace,
				}),
				LogicalPlan::CreateTableAsSelect {
					table,
					or_replace,
					source,
				} => {
					self.bind_create_table_as_select(table, *or_replace, source)
						.await
				}
				LogicalPlan::DropTable { name, if_exists } => Ok(ExecutionPlan::DropTable {
					name: name.clone(),
					if_exists: *if_exists,
				}),
				LogicalPlan::Delete { table, predicate } => {
					self.bind_delete(table, predicate.as_ref()).await
				}
				LogicalPlan::Insert {
					table,
					columns,
					rows,
				} => self.bind_insert(table, columns.as_deref(), rows).await,
				LogicalPlan::InsertSelect {
					table,
					columns,
					source,
				} => {
					self.bind_insert_select(table, columns.as_deref(), source)
						.await
				}
			}
		})
	}

	async fn bind_create_table_as_select(
		&self,
		table: &TableRef,
		or_replace: bool,
		source: &LogicalPlan,
	) -> Result<ExecutionPlan, BindingError> {
		let source = self.bind_plan(source).await?;
		let source_schema = source.schema();
		let schema = infer_ctas_schema(&source_schema)?;

		Ok(ExecutionPlan::CreateTableAsSelect {
			table: table.clone(),
			schema,
			or_replace,
			source: Box::new(source),
		})
	}

	async fn bind_delete(
		&self,
		table: &TableRef,
		predicate: Option<&Expr>,
	) -> Result<ExecutionPlan, BindingError> {
		let binding = self.catalog.resolve_table(table).await?;
		let bound_predicate = match predicate {
			Some(expr) => {
				let program =
					bind_expr_program(std::slice::from_ref(expr), &binding.schema, self.config)?;
				require_single_boolean_root(&program, "DELETE WHERE")?;
				Some(program)
			}
			None => None,
		};
		let pbc_program = bound_predicate.as_ref().and_then(compile_pbc);
		let profile_hint = match bound_predicate.as_ref() {
			Some(program) => {
				let scan = scan_plan(binding.clone(), None);
				scan_profile_hint(program, &scan).unwrap_or(QueryProfileHint::Scan)
			}
			None => QueryProfileHint::Scan,
		};

		Ok(ExecutionPlan::Delete {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema: binding.schema,
			predicate: bound_predicate,
			pbc_program,
			profile_hint,
		})
	}

	async fn bind_insert(
		&self,
		table: &TableRef,
		columns: Option<&[String]>,
		rows: &[Vec<InsertLiteral>],
	) -> Result<ExecutionPlan, BindingError> {
		let binding = self.catalog.resolve_table(table).await?;
		if rows.is_empty() {
			return Err(BindingError::UnsupportedExpression {
				message: "INSERT VALUES must contain at least one row".to_string(),
			});
		}
		if rows.len() > TABLE_WRITE_MAX_ROWS {
			return Err(BindingError::UnsupportedExpression {
				message: format!(
					"INSERT row count {} exceeds maximum {TABLE_WRITE_MAX_ROWS}",
					rows.len()
				),
			});
		}

		let schema = binding.schema.clone();
		let schema_fields = schema.fields();
		let target_columns = resolve_insert_target_columns(&schema, columns)?;
		let target_indices = target_columns
			.iter()
			.map(|column| column.0 as usize)
			.collect::<Vec<_>>();
		let expected_arity = target_indices.len();

		let mut bound_rows = Vec::with_capacity(rows.len());
		for (row_index, row) in rows.iter().enumerate() {
			if row.len() != expected_arity {
				return Err(BindingError::TypeMismatch {
					message: format!(
						"INSERT row {} has {} values; expected {expected_arity}",
						row_index + 1,
						row.len()
					),
				});
			}

			let bound_row = if columns.is_none() {
				row.iter()
					.zip(schema_fields.iter())
					.map(|(literal, field)| coerce_insert_literal(literal, field))
					.collect::<Result<Vec<_>, _>>()?
			} else {
				let mut values = vec![None; schema_fields.len()];
				for ((literal, target_index), _position) in
					row.iter().zip(target_indices.iter().copied()).zip(0..)
				{
					values[target_index] =
						Some(coerce_insert_literal(literal, schema.field(target_index))?);
				}
				for (field_index, value) in values.iter_mut().enumerate() {
					if value.is_none() {
						let field = schema.field(field_index);
						if !field.is_nullable() {
							return Err(BindingError::TypeMismatch {
								message: format!(
									"INSERT omits non-nullable column: {}",
									field.name()
								),
							});
						}
						*value = Some(typed_null_for_insert(field)?);
					}
				}
				values
					.into_iter()
					.map(|value| value.expect("all INSERT values filled"))
					.collect()
			};
			bound_rows.push(bound_row);
		}

		Ok(ExecutionPlan::Insert {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema,
			rows: bound_rows,
		})
	}

	async fn bind_insert_select(
		&self,
		table: &TableRef,
		columns: Option<&[String]>,
		source: &LogicalPlan,
	) -> Result<ExecutionPlan, BindingError> {
		let binding = self.catalog.resolve_table(table).await?;
		let schema = binding.schema.clone();
		let target_columns = resolve_insert_target_columns(&schema, columns)?;
		validate_omitted_insert_target_columns(&schema, &target_columns)?;
		let source = self.bind_plan(source).await?;
		let source_schema = source.schema();
		if source_schema.fields().len() != target_columns.len() {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"INSERT SELECT source has {} columns; expected {}",
					source_schema.fields().len(),
					target_columns.len()
				),
			});
		}

		for (source_index, target_column) in target_columns.iter().enumerate() {
			let source_field = source_schema.field(source_index);
			let target_field = schema.field(target_column.0 as usize);
			if !is_insert_select_coercible(source_field.data_type(), target_field.data_type()) {
				return Err(BindingError::TypeMismatch {
					message: format!(
						"INSERT SELECT source column {} type {:?} is not compatible with target column {} type {:?}",
						source_index + 1,
						source_field.data_type(),
						target_field.name(),
						target_field.data_type()
					),
				});
			}
		}

		Ok(ExecutionPlan::InsertSelect {
			legacy_table_id: binding.legacy_table_id,
			table: binding.table,
			fingerprint: binding.fingerprint,
			schema,
			target_columns,
			source: Box::new(source),
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

fn resolve_insert_target_columns(
	schema: &SchemaRef,
	columns: Option<&[String]>,
) -> Result<Vec<ColumnOrdinal>, BindingError> {
	let schema_fields = schema.fields();
	let target_columns = match columns {
		None => (0..schema_fields.len())
			.map(|index| ColumnOrdinal(u32::try_from(index).expect("column ordinal overflow")))
			.collect::<Vec<_>>(),
		Some(columns) => {
			let mut seen = HashSet::new();
			let mut target_columns = Vec::with_capacity(columns.len());
			for column in columns {
				if !seen.insert(column.clone()) {
					return Err(BindingError::UnsupportedExpression {
						message: format!("duplicate INSERT target column: {column}"),
					});
				}
				target_columns.push(resolve_column(column, schema)?.ordinal);
			}
			target_columns
		}
	};
	Ok(target_columns)
}

fn validate_omitted_insert_target_columns(
	schema: &SchemaRef,
	target_columns: &[ColumnOrdinal],
) -> Result<(), BindingError> {
	for (field_index, field) in schema.fields().iter().enumerate() {
		let ordinal = ColumnOrdinal(u32::try_from(field_index).expect("column ordinal overflow"));
		if !target_columns.contains(&ordinal) && !field.is_nullable() {
			return Err(BindingError::TypeMismatch {
				message: format!("INSERT omits non-nullable column: {}", field.name()),
			});
		}
	}
	Ok(())
}

fn infer_ctas_schema(source_schema: &SchemaRef) -> Result<SchemaRef, BindingError> {
	let mut seen = HashSet::new();
	let mut fields = Vec::with_capacity(source_schema.fields().len());
	for field in source_schema.fields() {
		validate_ctas_column_name(field.name())?;
		if !seen.insert(field.name().clone()) {
			return Err(BindingError::UnsupportedExpression {
				message: format!("duplicate CTAS output column: {}", field.name()),
			});
		}
		if !is_supported_ctas_column_type(field.data_type()) {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"unsupported CTAS output column type for {}: {:?}",
					field.name(),
					field.data_type()
				),
			});
		}
		fields.push(Field::new(
			field.name().clone(),
			field.data_type().clone(),
			field.is_nullable(),
		));
	}
	Ok(Arc::new(Schema::new(fields)))
}

fn validate_ctas_column_name(name: &str) -> Result<(), BindingError> {
	if name.is_empty() {
		return Err(BindingError::UnsupportedExpression {
			message: "CTAS output column name must not be empty".to_string(),
		});
	}
	if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&name) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("CTAS output column name is reserved for storage metadata: {name}"),
		});
	}
	Ok(())
}

fn is_supported_ctas_column_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
	)
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

fn compile_pbc(predicate: &BoundExprProgram) -> Option<PbcProgram> {
	PbcCompiler::new()
		.compile(Some(predicate), None)
		.ok()
		.map(PbcProgram)
}

fn attach_pbc_program(mut plan: ExecutionPlan, program: PbcProgram) -> (ExecutionPlan, bool) {
	match &mut plan {
		ExecutionPlan::Scan { pbc_program, .. } => {
			*pbc_program = Some(program);
			(plan, true)
		}
		_ => (plan, false),
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
		row_identity: crate::storage::RowIdentityProjection::None,
		profile_hint: QueryProfileHint::Scan,
		primary_key_columns: sorted_columns(&binding.primary_key_columns),
	}
}

fn sorted_columns(columns: &HashSet<ColumnOrdinal>) -> Vec<ColumnOrdinal> {
	let mut columns = columns.iter().copied().collect::<Vec<_>>();
	columns.sort();
	columns
}

fn scan_profile_hint(
	predicate: &BoundExprProgram,
	input: &ExecutionPlan,
) -> Option<QueryProfileHint> {
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

fn require_single_boolean_root(
	program: &BoundExprProgram,
	context: &'static str,
) -> Result<(), BindingError> {
	if program.root_count() != 1 {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"{context} predicate must have exactly one root, found {}",
				program.root_count()
			),
		});
	}
	let Some(root_type) = program.root_type(0) else {
		return Err(BindingError::TypeMismatch {
			message: format!("{context} predicate root type is missing"),
		});
	};
	if root_type == &DataType::Boolean {
		Ok(())
	} else {
		Err(BindingError::TypeMismatch {
			message: format!("{context} predicate must return Boolean, found {root_type:?}"),
		})
	}
}

fn apply_scan_profile_hint(plan: &mut ExecutionPlan, profile_hint: Option<QueryProfileHint>) {
	if let Some(next_profile_hint) = profile_hint
		&& let ExecutionPlan::Scan { profile_hint, .. } = plan
	{
		*profile_hint = next_profile_hint;
	}
}

fn predicate_includes_primary_key_equality(
	predicate: &BoundExprProgram,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	let Some(root) = predicate.single_root() else {
		return false;
	};
	predicate_node_includes_primary_key_equality(predicate, root, primary_key_columns)
}

fn predicate_node_includes_primary_key_equality(
	program: &BoundExprProgram,
	id: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	match program.node(id) {
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::Eq,
			left,
			right,
		}) => {
			primary_key_literal_equality(program, *left, *right, primary_key_columns)
				|| primary_key_literal_equality(program, *right, *left, primary_key_columns)
		}
		Some(BoundExprNode::Binary {
			op: BoundBinaryOp::And,
			left,
			right,
		}) => {
			predicate_node_includes_primary_key_equality(program, *left, primary_key_columns)
				|| predicate_node_includes_primary_key_equality(
					program,
					*right,
					primary_key_columns,
				)
		}
		_ => false,
	}
}

fn primary_key_literal_equality(
	program: &BoundExprProgram,
	column_side: BoundExprId,
	literal_side: BoundExprId,
	primary_key_columns: &[ColumnOrdinal],
) -> bool {
	let Some(BoundExprNode::Column(column)) = program.node(column_side) else {
		return false;
	};
	let Some(BoundExprNode::Literal(literal)) = program.node(literal_side) else {
		return false;
	};
	let Some(value) = program.literals.get(*literal as usize) else {
		return false;
	};
	primary_key_columns.contains(&column.ordinal) && literal_is_non_null(value)
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
		LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::DropTable { .. }
		| LogicalPlan::Delete { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. } => {}
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

fn terminal_exchange(plan: &ExecutionPlan) -> Option<(u32, &BoundExprProgram)> {
	match plan {
		ExecutionPlan::Exchange {
			partition_count,
			key,
			..
		} => Some((*partition_count, key)),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Sort { input, .. } => terminal_exchange(input),
		ExecutionPlan::Scan { .. }
		| ExecutionPlan::Join { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. } => None,
	}
}

fn exchange_key_ordinals(key: &BoundExprProgram) -> Vec<ColumnOrdinal> {
	key.root_columns()
		.unwrap_or_default()
		.into_iter()
		.map(|column| column.ordinal)
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
			row_identity,
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
					row_identity,
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
			collect_program_columns(&predicate, &mut child_required);
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let predicate = rewrite_program_ordinals(predicate, &remap)?;

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
			let requested_outputs =
				required_output.unwrap_or_else(|| output_columns(exprs.root_count()));
			let mut child_required = BTreeSet::new();
			for ordinal in &requested_outputs {
				let root = exprs.roots.get(ordinal.0 as usize).ok_or_else(|| {
					BindingError::UnsupportedExpression {
						message: format!("project output ordinal {:?} out of bounds", ordinal),
					}
				})?;
				collect_program_node_columns(&exprs, *root, &mut child_required);
			}
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let exprs = rewrite_program_ordinals(exprs, &remap)?;
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
			collect_program_columns(&group_expr, &mut child_required);
			for call in &aggr_expr {
				collect_program_columns(&call.args, &mut child_required);
			}

			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let group_expr = rewrite_program_ordinals(group_expr, &remap)?;
			let aggr_expr = aggr_expr
				.into_iter()
				.map(|call| {
					Ok(BoundAggregateCall {
						function: call.function,
						args: rewrite_program_ordinals(call.args, &remap)?,
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
			collect_program_columns(&key, &mut child_required);
			let (input, remap) = optimize_for_required(*input, Some(child_required))?;
			let key = rewrite_program_ordinals(key, &remap)?;
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
		ExecutionPlan::CreateTable {
			name,
			schema,
			if_not_exists,
			or_replace,
		} => Ok((
			ExecutionPlan::CreateTable {
				name,
				schema,
				if_not_exists,
				or_replace,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::CreateTableAsSelect {
			table,
			schema,
			or_replace,
			source,
		} => {
			let (source, _) = optimize_for_required(*source, None)?;
			Ok((
				ExecutionPlan::CreateTableAsSelect {
					table,
					schema,
					or_replace,
					source: Box::new(source),
				},
				OrdinalRemap::new(),
			))
		}
		ExecutionPlan::DropTable { name, if_exists } => Ok((
			ExecutionPlan::DropTable { name, if_exists },
			OrdinalRemap::new(),
		)),
		ExecutionPlan::Delete {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			predicate,
			pbc_program,
			profile_hint,
		} => Ok((
			ExecutionPlan::Delete {
				legacy_table_id,
				table,
				fingerprint,
				schema,
				predicate,
				pbc_program,
				profile_hint,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::Insert {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			rows,
		} => Ok((
			ExecutionPlan::Insert {
				legacy_table_id,
				table,
				fingerprint,
				schema,
				rows,
			},
			OrdinalRemap::new(),
		)),
		ExecutionPlan::InsertSelect {
			legacy_table_id,
			table,
			fingerprint,
			schema,
			target_columns,
			source,
		} => {
			let (source, _) = optimize_for_required(*source, None)?;
			Ok((
				ExecutionPlan::InsertSelect {
					legacy_table_id,
					table,
					fingerprint,
					schema,
					target_columns,
					source: Box::new(source),
				},
				OrdinalRemap::new(),
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

fn collect_program_columns(program: &BoundExprProgram, columns: &mut BTreeSet<ColumnOrdinal>) {
	for root in &program.roots {
		collect_program_node_columns(program, *root, columns);
	}
}

fn collect_program_node_columns(
	program: &BoundExprProgram,
	id: BoundExprId,
	columns: &mut BTreeSet<ColumnOrdinal>,
) {
	match program.node(id) {
		Some(BoundExprNode::Column(column)) => {
			columns.insert(column.ordinal);
		}
		Some(BoundExprNode::Literal(_)) | None => {}
		Some(BoundExprNode::Cast { input, .. })
		| Some(BoundExprNode::TryCast { input, .. })
		| Some(BoundExprNode::Unary { input, .. })
		| Some(BoundExprNode::TimeBucket { input, .. }) => {
			collect_program_node_columns(program, *input, columns);
		}
		Some(BoundExprNode::Binary { left, right, .. }) => {
			collect_program_node_columns(program, *left, columns);
			collect_program_node_columns(program, *right, columns);
		}
	}
}

fn rewrite_program_ordinals(
	mut program: BoundExprProgram,
	remap: &OrdinalRemap,
) -> Result<BoundExprProgram, BindingError> {
	for node in &mut program.nodes {
		if let BoundExprNode::Column(column) = node {
			column.ordinal =
				*remap
					.get(&column.ordinal)
					.ok_or_else(|| BindingError::UnsupportedExpression {
						message: format!("missing ordinal remap for {:?}", column.ordinal),
					})?;
		}
	}
	Ok(program)
}

fn bind_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<BoundAggregateCall, BindingError> {
	if matches!(function, AggregateFunction::CountAll) {
		return Ok(BoundAggregateCall {
			function: function.clone(),
			args: BoundExprProgramBuilder::new().build(),
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
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, config))
				.collect::<Result<Vec<_>, _>>()?;
			Ok(BoundAggregateCall {
				function: function.clone(),
				args: merge_aggregate_args(args),
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
				.map(|expr| bind_expr_program(std::slice::from_ref(expr), input_schema, config))
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

fn bind_stable_aggregate_call(
	function: &AggregateFunction,
	args: &[Expr],
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	spec: &AggregateKernelSpec,
) -> Result<BoundAggregateCall, BindingError> {
	if args.len() != aggregate_arity_len(spec.arity) {
		return Err(BindingError::UnsupportedExpression {
			message: format!("{} requires one argument", spec.sql_name),
		});
	}
	let args = bind_expr_program(args, input_schema, config)?;
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

fn merge_aggregate_args(programs: Vec<BoundExprProgram>) -> BoundExprProgram {
	if programs.is_empty() {
		return BoundExprProgramBuilder::new().build();
	}
	if programs.len() == 1 {
		return programs.into_iter().next().expect("one program");
	}
	let mut merged = BoundExprProgram {
		nodes: Vec::new(),
		node_types: Vec::new(),
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

fn offset_bound_node(node: BoundExprNode, node_offset: u32, literal_offset: u32) -> BoundExprNode {
	let offset_id = |id: BoundExprId| BoundExprId(id.0 + node_offset);
	match node {
		BoundExprNode::Column(column) => BoundExprNode::Column(column),
		BoundExprNode::Literal(literal) => BoundExprNode::Literal(literal + literal_offset),
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
		BoundExprNode::TimeBucket { interval, input } => BoundExprNode::TimeBucket {
			interval,
			input: offset_id(input),
		},
	}
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

fn bind_expr_program(
	exprs: &[Expr],
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<BoundExprProgram, BindingError> {
	let mut builder = BoundExprProgramBuilder::new();
	for expr in exprs {
		let root = bind_expr_into_program(&mut builder, expr, input_schema, config)?;
		builder.push_root(root);
	}
	Ok(builder.build())
}

fn bind_expr_into_program(
	builder: &mut BoundExprProgramBuilder,
	expr: &Expr,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
) -> Result<BoundExprId, BindingError> {
	match expr {
		Expr::Column(column_ref) => {
			Ok(builder.append_column(resolve_column(&column_ref.name.0, input_schema)?))
		}
		Expr::Literal(value) => Ok(builder.append_literal(value.clone())),
		Expr::Cast(inner, target) => {
			bind_explicit_cast(builder, input_schema, config, inner, target)
		}
		Expr::TryCast(inner, target) => bind_try_cast(builder, input_schema, config, inner, target),
		Expr::Add(left, right) => bind_arithmetic(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Add,
		),
		Expr::Sub(left, right) => bind_arithmetic(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Sub,
		),
		Expr::Mul(left, right) => bind_arithmetic(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Mul,
		),
		Expr::Div(left, right) => bind_arithmetic(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Div,
		),
		Expr::Eq(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Eq,
		),
		Expr::Neq(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Neq,
		),
		Expr::Lt(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Lt,
		),
		Expr::Lte(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Lte,
		),
		Expr::Gt(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Gt,
		),
		Expr::Gte(left, right) => bind_comparison(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Gte,
		),
		Expr::And(left, right) => bind_boolean(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::And,
		),
		Expr::Or(left, right) => bind_boolean(
			builder,
			input_schema,
			config,
			left,
			right,
			BoundBinaryOp::Or,
		),
		Expr::Not(inner) => {
			let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
			require_boolean(builder, "NOT", inner)?;
			Ok(builder.append_unary(BoundUnaryOp::Not, inner, DataType::Boolean))
		}
		Expr::IsNull(inner) => {
			let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
			Ok(builder.append_unary(BoundUnaryOp::IsNull, inner, DataType::Boolean))
		}
		Expr::IsNotNull(inner) => {
			let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
			Ok(builder.append_unary(BoundUnaryOp::IsNotNull, inner, DataType::Boolean))
		}
		Expr::TimeBucket(interval, inner) => {
			if !config.allow_experimental_kernels {
				return Err(BindingError::ExperimentalKernel {
					kernel: "TimeBucket",
				});
			}
			let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
			if builder.node_type(inner) != Some(&DataType::Int64) {
				return Err(BindingError::TypeMismatch {
					message: "TimeBucket expects Int64 (ms) input".to_string(),
				});
			}
			Ok(builder.append_time_bucket(*interval, inner, DataType::Int64))
		}
	}
}

fn bind_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	let left = bind_expr_into_program(builder, left, input_schema, config)?;
	let right = bind_expr_into_program(builder, right, input_schema, config)?;
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed left node");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed right node");
	let output_type =
		arithmetic_output_type(&left_type, &right_type).ok_or_else(|| {
			BindingError::TypeMismatch {
				message: format!(
					"arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and {right_type:?}"
				),
			}
		})?;
	let left = cast_if_needed(builder, left, &output_type)?;
	let right = cast_if_needed(builder, right, &output_type)?;
	Ok(builder.append_binary(op, left, right, output_type))
}

fn bind_explicit_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed cast input");
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(builder.append_cast(inner, target.clone(), target.clone()))
}

fn bind_try_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	let inner = bind_expr_into_program(builder, inner, input_schema, config)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed try-cast input");
	if !is_supported_explicit_numeric_cast(&source, target) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported TRY_CAST/SAFE_CAST: {source:?} to {target:?}; only Int32, Int64, Float32, and Float64 are supported"
			),
		});
	}
	Ok(builder.append_try_cast(inner, target.clone(), target.clone()))
}

fn cast_if_needed(
	builder: &mut BoundExprProgramBuilder,
	expr: BoundExprId,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	let source = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed cast input");
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
	Ok(builder.append_cast(expr, target.clone(), target.clone()))
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

fn is_insert_select_coercible(source: &DataType, target: &DataType) -> bool {
	source == target
		|| matches!(
			(source, target),
			(DataType::Int32, DataType::Int64)
				| (DataType::Int32, DataType::Float32)
				| (DataType::Int32, DataType::Float64)
				| (DataType::Int64, DataType::Int32)
				| (DataType::Int64, DataType::Float32)
				| (DataType::Int64, DataType::Float64)
				| (DataType::Float32, DataType::Int32)
				| (DataType::Float32, DataType::Int64)
				| (DataType::Float32, DataType::Float64)
				| (DataType::Float64, DataType::Int32)
				| (DataType::Float64, DataType::Int64)
				| (DataType::Float64, DataType::Float32)
		)
}

fn coerce_insert_literal(
	literal: &InsertLiteral,
	field: &Field,
) -> Result<ScalarValue, BindingError> {
	let target = field.data_type();
	let column = field.name();
	match literal {
		InsertLiteral::Null => {
			if field.is_nullable() {
				typed_null_for_insert(field)
			} else {
				Err(BindingError::TypeMismatch {
					message: format!("NULL is not allowed for non-nullable column: {column}"),
				})
			}
		}
		InsertLiteral::Boolean(value) => match target {
			DataType::Boolean => Ok(ScalarValue::Boolean(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Int64(value) => match target {
			DataType::Int32 => i32::try_from(*value)
				.map(|value| ScalarValue::Int32(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Int64 => Ok(ScalarValue::Int64(Some(*value))),
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value as f64))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::UInt64(value) => match target {
			DataType::Int32 => i32::try_from(*value)
				.map(|value| ScalarValue::Int32(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Int64 => i64::try_from(*value)
				.map(|value| ScalarValue::Int64(Some(value)))
				.map_err(|_| insert_overflow(column, literal, target)),
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value as f64))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Float64(value) => match target {
			DataType::Float32 => Ok(ScalarValue::Float32(Some(*value as f32))),
			DataType::Float64 => Ok(ScalarValue::Float64(Some(*value))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
		InsertLiteral::Utf8(value) => match target {
			DataType::Utf8 => Ok(ScalarValue::Utf8(Some(value.clone()))),
			_ => Err(insert_type_mismatch(column, literal, target)),
		},
	}
}

fn typed_null_for_insert(field: &Field) -> Result<ScalarValue, BindingError> {
	match field.data_type() {
		DataType::Boolean => Ok(ScalarValue::Boolean(None)),
		DataType::Int32 => Ok(ScalarValue::Int32(None)),
		DataType::Int64 => Ok(ScalarValue::Int64(None)),
		DataType::Float32 => Ok(ScalarValue::Float32(None)),
		DataType::Float64 => Ok(ScalarValue::Float64(None)),
		DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
		target => Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported INSERT target type for column {}: {target:?}",
				field.name()
			),
		}),
	}
}

fn insert_type_mismatch(column: &str, literal: &InsertLiteral, target: &DataType) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"INSERT value {literal:?} is not compatible with column {column} type {target:?}"
		),
	}
}

fn insert_overflow(column: &str, literal: &InsertLiteral, target: &DataType) -> BindingError {
	BindingError::TypeMismatch {
		message: format!("INSERT value {literal:?} overflows column {column} type {target:?}"),
	}
}

fn bind_comparison(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	let left = bind_expr_into_program(builder, left, input_schema, config)?;
	let right = bind_expr_into_program(builder, right, input_schema, config)?;
	coerce_comparison_literals(builder, left, right);
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed comparison left");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed comparison right");
	if left_type != right_type {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"comparison operands have different types: {:?} and {:?}",
				left_type, right_type
			),
		});
	}
	Ok(builder.append_binary(op, left, right, DataType::Boolean))
}

fn coerce_comparison_literals(
	builder: &mut BoundExprProgramBuilder,
	left: BoundExprId,
	right: BoundExprId,
) {
	if builder.literal(left).is_some() && builder.literal(right).is_some() {
		return;
	}
	if let Some(value) = builder.literal(left).cloned() {
		let Some(data_type) = builder.node_type(right).cloned() else {
			return;
		};
		let _ = builder.rewrite_literal(left, coerce_literal_value(value, &data_type));
		return;
	}
	if let Some(value) = builder.literal(right).cloned() {
		let Some(data_type) = builder.node_type(left).cloned() else {
			return;
		};
		let _ = builder.rewrite_literal(right, coerce_literal_value(value, &data_type));
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
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	config: &PlannerConfig,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	let left = bind_expr_into_program(builder, left, input_schema, config)?;
	let right = bind_expr_into_program(builder, right, input_schema, config)?;
	require_boolean(builder, "boolean operator left", left)?;
	require_boolean(builder, "boolean operator right", right)?;
	Ok(builder.append_binary(op, left, right, DataType::Boolean))
}

fn require_boolean(
	builder: &BoundExprProgramBuilder,
	context: &'static str,
	expr: BoundExprId,
) -> Result<(), BindingError> {
	let data_type = builder
		.node_type(expr)
		.cloned()
		.expect("builder returns typed boolean input");
	if data_type == DataType::Boolean {
		Ok(())
	} else {
		Err(BindingError::TypeMismatch {
			message: format!("{context} expected Boolean, got {data_type:?}"),
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

fn aggregate_schema(group_expr: &BoundExprProgram, aggr_expr: &[BoundAggregateCall]) -> SchemaRef {
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
			Field::new(name, data_type, nullable)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn schema_for_exprs_with_existing_names(
	exprs: &BoundExprProgram,
	existing_schema: &SchemaRef,
) -> SchemaRef {
	let fields = exprs
		.roots
		.iter()
		.enumerate()
		.map(|(idx, _)| {
			let name = existing_schema
				.fields()
				.get(idx)
				.map(|field| field.name().clone())
				.unwrap_or_else(|| {
					exprs
						.root_display_name(idx)
						.map(str::to_string)
						.unwrap_or_else(|| format!("col_{idx}"))
				});
			let data_type = exprs.root_type(idx).cloned().unwrap_or(DataType::Null);
			let nullable = exprs.root_nullable(idx).unwrap_or(true);
			Field::new(name, data_type, nullable)
		})
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn aggregate_field(call: &BoundAggregateCall) -> Field {
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
	use crate::ir::{BoundBinaryOp, BoundExprNode, QueryProfileHint, ScalarValue, TableId};
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
				let column = exprs.single_column_root().expect("project column root");
				assert_eq!(column.ordinal, ColumnOrdinal(0));
				assert_eq!(column.data_type, DataType::Int32);
				assert!(column.nullable);
				assert!(column.pbc_eligible);
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
				let column = exprs.single_column_root().expect("project column root");
				assert_eq!(column.ordinal, ColumnOrdinal(0));
				match *input {
					ExecutionPlan::Filter {
						input, predicate, ..
					} => {
						let root = predicate.single_root().expect("predicate root");
						match predicate.node(root).expect("predicate node") {
							BoundExprNode::Binary {
								op: BoundBinaryOp::Gt,
								left,
								..
							} => match predicate.node(*left).expect("predicate left") {
								BoundExprNode::Column(column) => {
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
