use super::*;
use crate::ir::{ScalarFunctionId, ScalarFunctionSignature};

pub(super) fn bind_scalar_function(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	id: ScalarFunctionId,
	args: &[Expr],
) -> Result<BoundExprId, BindingError> {
	let metadata = crate::ir::scalar_function_metadata(id);
	if !metadata.arity.accepts(args.len()) {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"{} expects {}, got {}",
				metadata.sql_name,
				metadata.arity.description(),
				args.len()
			),
		});
	}

	let bound_args = args
		.iter()
		.map(|arg| bind_expr_into_program(builder, arg, input_schema, ctx))
		.collect::<Result<Vec<_>, _>>()?;
	match metadata.signature {
		ScalarFunctionSignature::UnaryFloatPreserving => {
			let [input] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			if !builder
				.node_logical_type(*input)
				.is_some_and(LogicalTypeId::is_naked)
			{
				return Err(BindingError::TypeMismatch {
					message: format!("{} does not accept named logical types", metadata.sql_name),
				});
			}
			let output_type = match builder.node_type(*input) {
				Some(DataType::Float32) => DataType::Float32,
				Some(DataType::Float64) => DataType::Float64,
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects Float32 or Float64 input, got {actual:?}",
							metadata.sql_name
						),
					});
				}
				None => {
					return Err(BindingError::UnsupportedExpression {
						message: format!("{} input is not a bound expression", metadata.sql_name),
					});
				}
			};
			Ok(builder.append_scalar_function(id, bound_args, output_type))
		}
		ScalarFunctionSignature::BinaryFloatPreserving => {
			let [base, exponent] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			for operand in [base, exponent] {
				if !builder
					.node_logical_type(*operand)
					.is_some_and(LogicalTypeId::is_naked)
				{
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} does not accept named logical types",
							metadata.sql_name
						),
					});
				}
			}
			let output_type = match (builder.node_type(*base), builder.node_type(*exponent)) {
				(Some(DataType::Float32), Some(DataType::Float32)) => DataType::Float32,
				(Some(DataType::Float64), Some(DataType::Float64)) => DataType::Float64,
				(Some(base_type), Some(exponent_type)) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects two arguments of the same float type, got {base_type:?} and {exponent_type:?}",
							metadata.sql_name
						),
					});
				}
				_ => {
					return Err(BindingError::UnsupportedExpression {
						message: format!("{} input is not a bound expression", metadata.sql_name),
					});
				}
			};
			Ok(builder.append_scalar_function(id, bound_args, output_type))
		}
		ScalarFunctionSignature::UnaryNumericPreserving => {
			let [input] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			if !builder
				.node_logical_type(*input)
				.is_some_and(LogicalTypeId::is_naked)
			{
				return Err(BindingError::TypeMismatch {
					message: format!("{} does not accept named logical types", metadata.sql_name),
				});
			}
			let output_type = match builder.node_type(*input) {
				Some(
					data_type @ (DataType::Int32
					| DataType::Int64
					| DataType::Float32
					| DataType::Float64
					| DataType::Decimal128(_, _)),
				) => data_type.clone(),
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects a numeric input, got {actual:?}",
							metadata.sql_name
						),
					});
				}
				None => {
					return Err(BindingError::UnsupportedExpression {
						message: format!("{} input is not a bound expression", metadata.sql_name),
					});
				}
			};
			Ok(builder.append_scalar_function(id, bound_args, output_type))
		}
	}
}
