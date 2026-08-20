//! Toron's producer-owned conversion into KET-0006's shared value.
//!
//! This module deliberately sees only already-bound execution nodes. It neither
//! serializes private IR nor traverses a general relation plan.

use arrow::datatypes::DataType;
use boson::{
	CanonicalRelationExpression, GeneratedRange, NumericType, Projection, ScalarExpression,
	ScalarKind, V1_FORMAT_VERSION,
};

use crate::ir::{
	BoundBinaryOp, BoundExprId, BoundExprNode, BoundExprProgram, ExecutionPlan, LogicalTypeId,
	ScalarFunctionId, ScalarFunctionParameter, ScalarValue,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalLowerError {
	UnsupportedPlan,
	MalformedBoundProgram,
	UnsupportedExpression,
	UnsupportedDataType,
	NonNakedLogicalType,
	NullableExpression,
	SchemaMismatch,
}

impl std::fmt::Display for CanonicalLowerError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "{self:?}")
	}
}

impl std::error::Error for CanonicalLowerError {}

/// Convert the closed generated-relation subset after ordinary binding.
///
/// The materialization companion will call this immediately after binding its
/// CTAS source and before projection optimization. Keeping the conversion here
/// one-way prevents the shared contract from acquiring Toron's private plan.
pub(crate) fn try_canonical_generated_relation(
	plan: &ExecutionPlan,
) -> Result<CanonicalRelationExpression, CanonicalLowerError> {
	let relation = match plan {
		ExecutionPlan::Range { count, schema } => {
			validate_range_schema(schema)?;
			GeneratedRange {
				count: *count,
				projection: None,
			}
		}
		ExecutionPlan::Project {
			input,
			exprs,
			output_schema,
		} => {
			let ExecutionPlan::Range { count, schema } = input.as_ref() else {
				return Err(CanonicalLowerError::UnsupportedPlan);
			};
			validate_range_schema(schema)?;
			if exprs.roots.len() != output_schema.fields().len() {
				return Err(CanonicalLowerError::MalformedBoundProgram);
			}
			let mut projection = Vec::with_capacity(exprs.roots.len());
			for (index, root) in exprs.roots.iter().enumerate() {
				let field = output_schema.field(index);
				if field.is_nullable() {
					return Err(CanonicalLowerError::NullableExpression);
				}
				projection.push(Projection {
					output_name: field.name().to_owned(),
					expression: lower_scalar(exprs, *root)?,
				});
			}
			GeneratedRange {
				count: *count,
				projection: Some(projection),
			}
		}
		_ => return Err(CanonicalLowerError::UnsupportedPlan),
	};
	let expression = CanonicalRelationExpression {
		format_version: V1_FORMAT_VERSION,
		relation,
	};
	expression
		.validate_v1()
		.map_err(|_| CanonicalLowerError::UnsupportedExpression)?;
	Ok(expression)
}

fn validate_range_schema(schema: &arrow::datatypes::SchemaRef) -> Result<(), CanonicalLowerError> {
	let [field] = schema.fields().as_ref() else {
		return Err(CanonicalLowerError::SchemaMismatch);
	};
	if field.name() == "ordinal" && field.data_type() == &DataType::Int64 && !field.is_nullable() {
		Ok(())
	} else {
		Err(CanonicalLowerError::SchemaMismatch)
	}
}

fn lower_scalar(
	program: &BoundExprProgram,
	id: BoundExprId,
) -> Result<ScalarExpression, CanonicalLowerError> {
	if !program
		.node_logical_type(id)
		.is_some_and(LogicalTypeId::is_naked)
	{
		return Err(CanonicalLowerError::NonNakedLogicalType);
	}
	if program.node_nullable(id) != Some(false) {
		return Err(CanonicalLowerError::NullableExpression);
	}
	let data_type = numeric_type(
		program
			.node_type(id)
			.ok_or(CanonicalLowerError::MalformedBoundProgram)?,
	)?;
	let kind = match program
		.node(id)
		.ok_or(CanonicalLowerError::MalformedBoundProgram)?
	{
		BoundExprNode::Column(column)
			if column.ordinal.0 == 0
				&& column.name.0 == "ordinal"
				&& column.data_type == DataType::Int64
				&& !column.nullable =>
		{
			ScalarKind::Ordinal
		}
		BoundExprNode::Literal(literal) => lower_literal(
			program
				.literals
				.get(*literal as usize)
				.ok_or(CanonicalLowerError::MalformedBoundProgram)?,
		)?,
		BoundExprNode::Cast { input, target } => {
			if numeric_type(target)? != data_type {
				return Err(CanonicalLowerError::MalformedBoundProgram);
			}
			ScalarKind::Cast(Box::new(lower_scalar(program, *input)?))
		}
		BoundExprNode::Binary { op, left, right } => {
			let left = Box::new(lower_scalar(program, *left)?);
			let right = Box::new(lower_scalar(program, *right)?);
			if left.data_type != data_type || right.data_type != data_type {
				return Err(CanonicalLowerError::MalformedBoundProgram);
			}
			match op {
				BoundBinaryOp::Add => ScalarKind::Add(left, right),
				BoundBinaryOp::Sub => ScalarKind::Sub(left, right),
				BoundBinaryOp::Mul => ScalarKind::Mul(left, right),
				BoundBinaryOp::Div => ScalarKind::Div(left, right),
				_ => return Err(CanonicalLowerError::UnsupportedExpression),
			}
		}
		BoundExprNode::ScalarFunction {
			id: ScalarFunctionId::Floor,
			args,
			parameter: ScalarFunctionParameter::None,
		} if args.len() == 1 => {
			let child = Box::new(lower_scalar(program, args[0])?);
			if child.data_type != data_type {
				return Err(CanonicalLowerError::MalformedBoundProgram);
			}
			ScalarKind::Floor(child)
		}
		BoundExprNode::ScalarFunction {
			id: ScalarFunctionId::SeededRandom,
			args,
			parameter: ScalarFunctionParameter::None,
		} if args.len() == 2 => {
			let seed = Box::new(lower_scalar(program, args[0])?);
			let key = Box::new(lower_scalar(program, args[1])?);
			if data_type != NumericType::Float64
				|| seed.data_type != NumericType::Int64
				|| key.data_type != NumericType::Int64
			{
				return Err(CanonicalLowerError::MalformedBoundProgram);
			}
			ScalarKind::SeededUnitInterval(seed, key)
		}
		_ => return Err(CanonicalLowerError::UnsupportedExpression),
	};
	Ok(ScalarExpression { data_type, kind })
}

fn lower_literal(value: &ScalarValue) -> Result<ScalarKind, CanonicalLowerError> {
	match value {
		ScalarValue::Int32(Some(value)) => Ok(ScalarKind::Int32Literal(*value)),
		ScalarValue::Int64(Some(value)) => Ok(ScalarKind::Int64Literal(*value)),
		ScalarValue::Float64(Some(value)) if value.is_finite() => {
			Ok(ScalarKind::Float64Literal(value.to_bits()))
		}
		ScalarValue::Float64(Some(_)) => Err(CanonicalLowerError::UnsupportedExpression),
		_ => Err(CanonicalLowerError::UnsupportedExpression),
	}
}

fn numeric_type(data_type: &DataType) -> Result<NumericType, CanonicalLowerError> {
	match data_type {
		DataType::Int32 => Ok(NumericType::Int32),
		DataType::Int64 => Ok(NumericType::Int64),
		DataType::Float64 => Ok(NumericType::Float64),
		_ => Err(CanonicalLowerError::UnsupportedDataType),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::datatypes::{Field, Schema};
	use std::sync::Arc;

	use crate::ir::{
		BoundExprProgramBuilder, ColumnName, ColumnOrdinal, ResolvedColumn, range_relation_schema,
	};

	fn range() -> ExecutionPlan {
		ExecutionPlan::Range {
			count: 4,
			schema: range_relation_schema(),
		}
	}

	fn ordinal(builder: &mut BoundExprProgramBuilder) -> BoundExprId {
		builder.append_column(ResolvedColumn {
			name: ColumnName("ordinal".to_owned()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: false,
		})
	}

	#[test]
	fn bare_range_becomes_an_absent_projection() {
		let canonical = try_canonical_generated_relation(&range()).expect("lower range");
		assert_eq!(canonical.relation.count, 4);
		assert_eq!(canonical.relation.projection, None);
	}

	#[test]
	fn admitted_project_preserves_bound_name_cast_and_seeded_tree() {
		let mut builder = BoundExprProgramBuilder::new();
		let seed = builder.append_literal(ScalarValue::Int64(Some(11)));
		let ordinal = ordinal(&mut builder);
		let seeded = builder.append_scalar_function(
			ScalarFunctionId::SeededRandom,
			vec![seed, ordinal],
			DataType::Float64,
		);
		let four = builder.append_literal(ScalarValue::Int32(Some(4)));
		let four = builder.append_cast(four, DataType::Float64, DataType::Float64);
		let product = builder.append_binary(BoundBinaryOp::Mul, seeded, four, DataType::Float64);
		let floor = builder.append_scalar_function(
			ScalarFunctionId::Floor,
			vec![product],
			DataType::Float64,
		);
		builder.push_root(floor);
		let program = builder.build();
		let plan = ExecutionPlan::Project {
			input: Box::new(range()),
			exprs: program,
			output_schema: Arc::new(Schema::new(vec![Field::new(
				"fixture",
				DataType::Float64,
				false,
			)])),
		};

		let canonical = try_canonical_generated_relation(&plan).expect("lower project");
		let projection = canonical.relation.projection.expect("present projection");
		assert_eq!(projection[0].output_name, "fixture");
		assert_eq!(
			projection[0].expression.kind,
			ScalarKind::Floor(Box::new(ScalarExpression {
				data_type: NumericType::Float64,
				kind: ScalarKind::Mul(
					Box::new(ScalarExpression {
						data_type: NumericType::Float64,
						kind: ScalarKind::SeededUnitInterval(
							Box::new(ScalarExpression {
								data_type: NumericType::Int64,
								kind: ScalarKind::Int64Literal(11),
							}),
							Box::new(ScalarExpression {
								data_type: NumericType::Int64,
								kind: ScalarKind::Ordinal,
							}),
						),
					}),
					Box::new(ScalarExpression {
						data_type: NumericType::Float64,
						kind: ScalarKind::Cast(Box::new(ScalarExpression {
							data_type: NumericType::Int32,
							kind: ScalarKind::Int32Literal(4),
						})),
					}),
				),
			}))
		);
	}

	#[test]
	fn non_naked_logical_type_refuses() {
		let mut builder = BoundExprProgramBuilder::new();
		let ordinal = ordinal(&mut builder);
		assert!(builder.set_node_logical_type(ordinal, LogicalTypeId(9)));
		builder.push_root(ordinal);
		let plan = ExecutionPlan::Project {
			input: Box::new(range()),
			exprs: builder.build(),
			output_schema: Arc::new(Schema::new(vec![Field::new(
				"ordinal",
				DataType::Int64,
				false,
			)])),
		};
		assert_eq!(
			try_canonical_generated_relation(&plan),
			Err(CanonicalLowerError::NonNakedLogicalType)
		);
	}
}
