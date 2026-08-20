//! Explicit `CAST`/`TRY_CAST` binding (ADR 564): admission against the
//! support table, and the text-to-temporal rewrite onto the ADR 558 kernels.

use super::*;

pub(super) fn bind_explicit_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(inner) {
		return resolve_untyped_null(builder, target);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	coerce_decimal_literal_to_legacy_explicit_numeric_source(
		builder,
		inner,
		target,
		ctx.session_timezone,
	)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed cast input");
	sql_cast_admissible(&source, target, "CAST")?;
	// ADR 564: text-to-temporal casts bind onto the ADR 558 kernels, so
	// `CAST(text AS TIMESTAMPTZ)` and `TO_TIMESTAMP(text)` are one node and
	// cannot diverge; the session zone rides the kernel's payload.
	if let Some(node) = bind_text_temporal_cast(builder, ctx, inner, &source, target, false)? {
		return Ok(node);
	}
	Ok(builder.append_cast(inner, target.clone(), target.clone()))
}

/// ADR 564: one admission check reading the support table. A pair the table
/// does not declare, or declares synthesized-only, is not user SQL. Identity
/// pairs short-circuit before any lookup and are not listed in the table; the
/// bind functions still append their cast node, so nullability metadata is
/// unchanged by admission.
fn sql_cast_admissible(
	source: &DataType,
	target: &DataType,
	spelling: &str,
) -> Result<(), BindingError> {
	if source == target {
		return Ok(());
	}
	let admissible = crate::cast_support::cast_family(source)
		.zip(crate::cast_support::cast_family(target))
		.and_then(|(source, target)| crate::cast_support::explicit_cast_rule(source, target))
		.is_some_and(|(_, admission)| {
			matches!(admission, crate::cast_support::CastAdmission::SqlCast)
		});
	if admissible {
		return Ok(());
	}
	Err(BindingError::TypeMismatch {
		message: format!(
			"unsupported {spelling}: {source:?} to {target:?}; the supported conversions are \
			 numeric-to-numeric, text-to-numeric, text-to-temporal, and value-to-text"
		),
	})
}

/// Binds `CAST(text AS TIMESTAMPTZ | DATE)` onto the ADR 558 parse kernels.
/// Returns `None` when the pair is not a text-to-temporal cast.
fn bind_text_temporal_cast(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	inner: BoundExprId,
	source: &DataType,
	target: &DataType,
	lenient: bool,
) -> Result<Option<BoundExprId>, BindingError> {
	if !matches!(source, DataType::Utf8 | DataType::LargeUtf8) {
		return Ok(None);
	}
	match target {
		DataType::Date32 => {
			let id = if lenient {
				crate::ir::ScalarFunctionId::TryToDate
			} else {
				crate::ir::ScalarFunctionId::ToDate
			};
			Ok(Some(builder.append_scalar_function(
				id,
				vec![inner],
				DataType::Date32,
			)))
		}
		DataType::Timestamp(unit, _) => {
			// The parse kernel produces nanoseconds; a coarser declared unit
			// would lie about the array it labels.
			if !matches!(unit, TimeUnit::Nanosecond) {
				return Err(BindingError::TypeMismatch {
					message: "CAST from text yields nanosecond precision; use TIMESTAMPTZ or \
					          TIMESTAMPTZ(9)"
						.to_string(),
				});
			}
			let id = if lenient {
				crate::ir::ScalarFunctionId::TryToTimestamp
			} else {
				crate::ir::ScalarFunctionId::ToTimestamp
			};
			Ok(Some(builder.append_parameterized_scalar_function(
				id,
				vec![inner],
				crate::ir::ScalarFunctionParameter::ToTimestamp {
					zone: ctx.session_timezone.name().into(),
				},
				target.clone(),
			)))
		}
		_ => Ok(None),
	}
}

pub(super) fn bind_try_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	target: &DataType,
) -> Result<BoundExprId, BindingError> {
	if is_untyped_null(inner) {
		return resolve_untyped_null(builder, target);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	coerce_decimal_literal_to_legacy_explicit_numeric_source(
		builder,
		inner,
		target,
		ctx.session_timezone,
	)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed try-cast input");
	sql_cast_admissible(&source, target, "TRY_CAST/SAFE_CAST")?;
	if let Some(node) = bind_text_temporal_cast(builder, ctx, inner, &source, target, true)? {
		return Ok(node);
	}
	Ok(builder.append_try_cast(inner, target.clone(), target.clone()))
}
