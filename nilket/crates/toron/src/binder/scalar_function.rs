use super::*;
use crate::ir::{ScalarFunctionId, ScalarFunctionParameter, ScalarFunctionSignature};

pub(super) fn bind_scalar_function(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	id: ScalarFunctionId,
	args: &[Expr],
	parameter: &ScalarFunctionParameter,
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
	match metadata.signature {
		ScalarFunctionSignature::TemporalExtract => {
			let [inner] = args else {
				unreachable!("the registry arity was checked before binding")
			};
			let ScalarFunctionParameter::Extract(payload) = parameter else {
				return Err(BindingError::TypeMismatch {
					message: "EXTRACT requires an extract field parameter".to_string(),
				});
			};
			return super::arithmetic::bind_temporal_extract(
				builder,
				input_schema,
				ctx,
				id,
				payload.field,
				inner,
			);
		}
		ScalarFunctionSignature::TemporalTrunc => {
			let [inner] = args else {
				unreachable!("the registry arity was checked before binding")
			};
			let ScalarFunctionParameter::DateTrunc(payload) = parameter else {
				return Err(BindingError::TypeMismatch {
					message: "DATE_TRUNC requires a date_trunc field parameter".to_string(),
				});
			};
			return super::arithmetic::bind_date_trunc(
				builder,
				input_schema,
				ctx,
				id,
				payload.field,
				inner,
			);
		}
		_ => {}
	}
	match (id, parameter) {
		(_, ScalarFunctionParameter::None)
		| (ScalarFunctionId::Trim, ScalarFunctionParameter::Trim(_)) => {}
		(ScalarFunctionId::Trim, _) => {
			return Err(BindingError::TypeMismatch {
				message: "TRIM requires a TRIM specification parameter".to_string(),
			});
		}
		(_, _) => {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"{} does not accept a scalar-function parameter",
					metadata.sql_name
				),
			});
		}
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
		ScalarFunctionSignature::UnaryStringPreserving
		| ScalarFunctionSignature::StringToInteger => {
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
			let input_type = match builder.node_type(*input) {
				Some(data_type @ (DataType::Utf8 | DataType::LargeUtf8)) => data_type.clone(),
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects a string input, got {actual:?}",
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
			// A length is a count, not a narrower string. Int64 regardless of the
			// input's offset width, so the result type does not leak how the
			// column happens to be stored.
			let output_type = match metadata.signature {
				ScalarFunctionSignature::StringToInteger => DataType::Int64,
				_ => input_type,
			};
			Ok(builder.append_scalar_function(id, bound_args, output_type))
		}
		ScalarFunctionSignature::StringWithOptionalString => {
			let (input, characters) = match bound_args.as_slice() {
				[input] => (input, None),
				[input, characters] => (input, Some(characters)),
				_ => unreachable!("the registry arity was checked before binding"),
			};
			for operand in std::iter::once(input).chain(characters) {
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
			let input_type = match builder.node_type(*input) {
				Some(data_type @ (DataType::Utf8 | DataType::LargeUtf8)) => data_type.clone(),
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects a string input, got {actual:?}",
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
			if let Some(characters) = characters {
				match builder.node_type(*characters) {
					Some(actual) if *actual == input_type => {}
					Some(actual) => {
						return Err(BindingError::TypeMismatch {
							message: format!(
								"{} expects its character set to share the input string type, got {input_type:?} and {actual:?}",
								metadata.sql_name
							),
						});
					}
					None => {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"{} character set is not a bound expression",
								metadata.sql_name
							),
						});
					}
				}
			}
			match parameter {
				ScalarFunctionParameter::None => {
					Ok(builder.append_scalar_function(id, bound_args, input_type))
				}
				ScalarFunctionParameter::Trim(_) => Ok(builder
					.append_parameterized_scalar_function(
						id,
						bound_args,
						parameter.clone(),
						input_type,
					)),
				_ => Err(BindingError::TypeMismatch {
					message: format!(
						"{} requires a TRIM specification parameter",
						metadata.sql_name
					),
				}),
			}
		}
		ScalarFunctionSignature::StringWithConstantBounds => {
			let (input, from, length) = match bound_args.as_slice() {
				[input, from] => (input, from, None),
				[input, from, length] => (input, from, Some(length)),
				_ => unreachable!("the registry arity was checked before binding"),
			};
			if !builder
				.node_logical_type(*input)
				.is_some_and(LogicalTypeId::is_naked)
			{
				return Err(BindingError::TypeMismatch {
					message: format!("{} does not accept named logical types", metadata.sql_name),
				});
			}
			// A bound is a shape parameter, not a value the rows carry. Requiring
			// a literal is what lets the executor read it once instead of per row,
			// and it preserves the contract the frontend has always enforced.
			let constant_bound = |id: BoundExprId, role: &str| -> Result<i64, BindingError> {
				match builder.literal(id) {
					Some(ScalarValue::Int32(Some(value))) => Ok(i64::from(*value)),
					Some(ScalarValue::Int64(Some(value))) => Ok(*value),
					_ => Err(BindingError::TypeMismatch {
						message: format!("{} {role} must be an integer literal", metadata.sql_name),
					}),
				}
			};
			if constant_bound(*from, "FROM")? < 1 {
				return Err(BindingError::TypeMismatch {
					message: format!("{} FROM must be >= 1", metadata.sql_name),
				});
			}
			if let Some(length) = length
				&& constant_bound(*length, "FOR")? < 0
			{
				return Err(BindingError::TypeMismatch {
					message: format!("{} FOR must be >= 0", metadata.sql_name),
				});
			}
			// Utf8 only, deliberately. The pre-registry binder accepted no other
			// string type, and widening to LargeUtf8 is a capability decision
			// that must not ride along inside a migration.
			let output_type = match builder.node_type(*input) {
				Some(DataType::Utf8) => DataType::Utf8,
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects Utf8 input, got {actual:?}",
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
		ScalarFunctionSignature::Int64MillisWithConstantInterval => {
			let [interval, input] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			// A named logical type over Int64 carries units that are not Unix
			// milliseconds; bucketing one would silently reinterpret it.
			if !builder
				.node_logical_type(*input)
				.is_some_and(LogicalTypeId::is_naked)
			{
				return Err(BindingError::TypeMismatch {
					message: "TimeBucket does not accept named logical types".to_string(),
				});
			}
			if builder.node_type(*input) != Some(&DataType::Int64) {
				return Err(BindingError::TypeMismatch {
					message: "TimeBucket expects Int64 (ms) input".to_string(),
				});
			}
			// The interval must be constant so the executor can read it once.
			// Positivity is deliberately *not* checked here: it has always been
			// an evaluation-time refusal, and moving it earlier would change the
			// contract inside a migration.
			if !matches!(builder.literal(*interval), Some(ScalarValue::Interval(_))) {
				return Err(BindingError::TypeMismatch {
					message: "TIME_BUCKET interval must be an interval literal".to_string(),
				});
			}
			Ok(builder.append_scalar_function(id, bound_args, DataType::Int64))
		}
		ScalarFunctionSignature::TimestampWithConstantZone => {
			let [input, zone] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			let Some(ScalarValue::Utf8(Some(zone))) = builder.literal(*zone).cloned() else {
				return Err(BindingError::TypeMismatch {
					message: "AT TIME ZONE expects a string literal timezone".to_string(),
				});
			};
			let zone = super::arithmetic::validate_temporal_zone(&zone)?;
			let input_type = builder.node_type(*input).cloned().ok_or_else(|| {
				BindingError::UnsupportedExpression {
					message: "AT TIME ZONE input is malformed".to_string(),
				}
			})?;
			let unit = super::arithmetic::supported_timestamp_unit(&input_type, "AT TIME ZONE")?;
			// The zone's *value* becomes part of the result type. Every other rule
			// derives its output from argument types alone; this one reads the
			// constant, which is what this migration set out to test.
			Ok(builder.append_scalar_function(
				id,
				bound_args,
				DataType::Timestamp(unit, Some(zone.into())),
			))
		}
		ScalarFunctionSignature::NiladicFloat => {
			// Nothing to type-check: the arity gate already proved there are no
			// arguments, and the output type is the registration's own.
			Ok(builder.append_scalar_function(id, bound_args, DataType::Float64))
		}
		ScalarFunctionSignature::VariadicOrderedSameType => {
			let [first, rest @ ..] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			for argument in bound_args.iter() {
				if !builder
					.node_logical_type(*argument)
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
			// Same literal rule the rest of the engine uses: a decimal literal
			// adapts to an adjacent float, a column never does.
			for argument in rest {
				let (Some(first_type), Some(argument_type)) =
					(builder.node_type(*first), builder.node_type(*argument))
				else {
					continue;
				};
				let (first_type, argument_type) = (first_type.clone(), argument_type.clone());
				super::arithmetic::coerce_decimal_arithmetic_literal(
					builder,
					ctx,
					*first,
					*argument,
					&first_type,
					&argument_type,
				)?;
			}
			let Some(output_type) = builder.node_type(*first).cloned() else {
				return Err(BindingError::UnsupportedExpression {
					message: format!("{} input is not a bound expression", metadata.sql_name),
				});
			};
			if !matches!(
				output_type,
				DataType::Int32
					| DataType::Int64
					| DataType::Float32
					| DataType::Float64
					| DataType::Decimal128(_, _)
					| DataType::Utf8
					| DataType::LargeUtf8
			) {
				return Err(BindingError::TypeMismatch {
					message: format!(
						"{} expects an ordered numeric or string type, got {output_type:?}",
						metadata.sql_name
					),
				});
			}
			// Comparing across types would make the answer depend on a promotion
			// nobody asked for, so every argument must already agree.
			for argument in rest {
				match builder.node_type(*argument) {
					Some(argument_type) if *argument_type == output_type => {}
					Some(argument_type) => {
						return Err(BindingError::TypeMismatch {
							message: format!(
								"{} expects every argument to share one type, got {output_type:?} and {argument_type:?}",
								metadata.sql_name
							),
						});
					}
					None => {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"{} input is not a bound expression",
								metadata.sql_name
							),
						});
					}
				}
			}
			Ok(builder.append_scalar_function(id, bound_args, output_type))
		}
		ScalarFunctionSignature::NumericBucketing => {
			let [operand, low, high, count] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			for argument in bound_args.iter() {
				if !builder
					.node_logical_type(*argument)
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
			// `WIDTH_BUCKET(price, 0.0, 100.0, 4)` is the natural spelling, and a
			// SQL decimal literal binds as Decimal128. Reuse the engine's existing
			// rule — a literal adapts to an adjacent float, a column never does —
			// rather than making this function demand casts nothing else demands.
			for bound in [low, high] {
				let (Some(operand_type), Some(bound_type)) =
					(builder.node_type(*operand), builder.node_type(*bound))
				else {
					continue;
				};
				let (operand_type, bound_type) = (operand_type.clone(), bound_type.clone());
				super::arithmetic::coerce_decimal_arithmetic_literal(
					builder,
					ctx,
					*operand,
					*bound,
					&operand_type,
					&bound_type,
				)?;
			}
			// The three range operands must agree; a mixed-width range would make
			// the bucket width depend on which operand won a promotion.
			match (
				builder.node_type(*operand),
				builder.node_type(*low),
				builder.node_type(*high),
			) {
				(Some(DataType::Float32), Some(DataType::Float32), Some(DataType::Float32))
				| (Some(DataType::Float64), Some(DataType::Float64), Some(DataType::Float64)) => {}
				(Some(operand_type), Some(low_type), Some(high_type)) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects operand, low, and high to share one float type, got {operand_type:?}, {low_type:?}, and {high_type:?}",
							metadata.sql_name
						),
					});
				}
				_ => {
					return Err(BindingError::UnsupportedExpression {
						message: format!("{} input is not a bound expression", metadata.sql_name),
					});
				}
			}
			match builder.node_type(*count) {
				Some(DataType::Int32 | DataType::Int64) => {}
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects an integer bucket count, got {actual:?}",
							metadata.sql_name
						),
					});
				}
				None => {
					return Err(BindingError::UnsupportedExpression {
						message: format!(
							"{} bucket count is not a bound expression",
							metadata.sql_name
						),
					});
				}
			}
			// A bucket ordinal is a count, not a measurement in the operand's
			// units, so it is Int64 whatever the range's width.
			Ok(builder.append_scalar_function(id, bound_args, DataType::Int64))
		}
		ScalarFunctionSignature::BinaryIntegerPreserving => {
			let [left, right] = bound_args.as_slice() else {
				unreachable!("the registry arity was checked before binding")
			};
			for operand in [left, right] {
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
			let output_type = match (builder.node_type(*left), builder.node_type(*right)) {
				(Some(DataType::Int32), Some(DataType::Int32)) => DataType::Int32,
				(Some(DataType::Int64), Some(DataType::Int64)) => DataType::Int64,
				(Some(left_type), Some(right_type)) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects two arguments of the same integer type, got {left_type:?} and {right_type:?}",
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
		ScalarFunctionSignature::FloatWithOptionalIntegerDigits => {
			let (value, digits) = match bound_args.as_slice() {
				[value] => (value, None),
				[value, digits] => (value, Some(digits)),
				_ => unreachable!("the registry arity was checked before binding"),
			};
			for operand in std::iter::once(value).chain(digits) {
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
			// The digit count selects a scale; it is not a second value, so it
			// must never participate in the output type.
			if let Some(digits) = digits {
				match builder.node_type(*digits) {
					Some(DataType::Int32 | DataType::Int64) => {}
					Some(actual) => {
						return Err(BindingError::TypeMismatch {
							message: format!(
								"{} expects an integer digit count, got {actual:?}",
								metadata.sql_name
							),
						});
					}
					None => {
						return Err(BindingError::UnsupportedExpression {
							message: format!(
								"{} digit count is not a bound expression",
								metadata.sql_name
							),
						});
					}
				}
			}
			let output_type = match builder.node_type(*value) {
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
		ScalarFunctionSignature::UnaryIntegerOrFloatPreserving => {
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
					| DataType::Float64),
				) => data_type.clone(),
				Some(actual) => {
					return Err(BindingError::TypeMismatch {
						message: format!(
							"{} expects Int32, Int64, Float32, or Float64 input, got {actual:?}",
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
		ScalarFunctionSignature::TemporalExtract | ScalarFunctionSignature::TemporalTrunc => {
			unreachable!("temporal signatures return through the parameterized binding path")
		}
	}
}
