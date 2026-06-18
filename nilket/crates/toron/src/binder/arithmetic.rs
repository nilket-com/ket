use super::*;

pub(super) fn bind_at_time_zone(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	zone: &str,
) -> Result<BoundExprId, BindingError> {
	let zone = validate_temporal_zone(zone)?;
	let input = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let input_type =
		builder
			.node_type(input)
			.cloned()
			.ok_or_else(|| BindingError::UnsupportedExpression {
				message: "AT TIME ZONE input is malformed".to_string(),
			})?;
	let unit = supported_timestamp_unit(&input_type, "AT TIME ZONE")?;
	Ok(builder.append_at_time_zone(
		input,
		zone.clone(),
		DataType::Timestamp(unit, Some(zone.into())),
	))
}

pub(super) fn bind_temporal_extract(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	field: TemporalExtractField,
	inner: &Expr,
) -> Result<BoundExprId, BindingError> {
	let (zone, operand, explicit_zone) = temporal_operand_zone(ctx, inner)?;
	let input = bind_expr_into_program(builder, operand, input_schema, ctx)?;
	let input_type =
		builder
			.node_type(input)
			.cloned()
			.ok_or_else(|| BindingError::UnsupportedExpression {
				message: "EXTRACT input is malformed".to_string(),
			})?;
	match input_type {
		DataType::Date32 if explicit_zone => Err(BindingError::TypeMismatch {
			message: "AT TIME ZONE requires Timestamp".to_string(),
		}),
		DataType::Date32 => {
			if !field.is_date32_supported() {
				return Err(BindingError::TypeMismatch {
					message: format!("EXTRACT {} is not supported for Date32", field.as_str()),
				});
			}
			Ok(builder.append_extract(field, None, input, DataType::Int32))
		}
		DataType::Date64 => Err(BindingError::TypeMismatch {
			message: "EXTRACT is not supported for Date64".to_string(),
		}),
		data_type @ DataType::Timestamp(_, _) => {
			supported_timestamp_unit(&data_type, "EXTRACT")?;
			Ok(builder.append_extract(field, Some(zone), input, DataType::Int32))
		}
		other => Err(BindingError::TypeMismatch {
			message: format!("EXTRACT expects Timestamp or Date32 input, got {other:?}"),
		}),
	}
}

pub(super) fn bind_date_trunc(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	field: TemporalTruncField,
	inner: &Expr,
) -> Result<BoundExprId, BindingError> {
	let (zone, operand, explicit_zone) = temporal_operand_zone(ctx, inner)?;
	let input = bind_expr_into_program(builder, operand, input_schema, ctx)?;
	let input_type =
		builder
			.node_type(input)
			.cloned()
			.ok_or_else(|| BindingError::UnsupportedExpression {
				message: "date_trunc input is malformed".to_string(),
			})?;
	match input_type {
		DataType::Date32 if explicit_zone => Err(BindingError::TypeMismatch {
			message: "AT TIME ZONE requires Timestamp".to_string(),
		}),
		DataType::Date32 => Err(BindingError::TypeMismatch {
			message: "Date32 date_trunc is deferred in ADR 151".to_string(),
		}),
		DataType::Date64 => Err(BindingError::TypeMismatch {
			message: "date_trunc is not supported for Date64".to_string(),
		}),
		data_type @ DataType::Timestamp(_, _) => {
			let unit = supported_timestamp_unit(&data_type, "date_trunc")?;
			Ok(builder.append_date_trunc(
				field,
				zone,
				input,
				DataType::Timestamp(unit, Some("UTC".into())),
			))
		}
		other => Err(BindingError::TypeMismatch {
			message: format!("date_trunc expects Timestamp input, got {other:?}"),
		}),
	}
}

pub(super) fn temporal_operand_zone<'a>(
	ctx: &ExprBindContext<'_>,
	inner: &'a Expr,
) -> Result<(String, &'a Expr, bool), BindingError> {
	match inner {
		Expr::AtTimeZone(timestamp, zone) => Ok((validate_temporal_zone(zone)?, timestamp, true)),
		_ => Ok((ctx.session_timezone.to_string(), inner, false)),
	}
}

pub(super) fn calendar_arithmetic_timezone(
	ctx: &ExprBindContext<'_>,
	left_expr: &Expr,
	right_expr: &Expr,
	left_type: &DataType,
	right_type: &DataType,
) -> Result<Tz, BindingError> {
	let zone = if matches!(left_type, DataType::Timestamp(_, _)) {
		temporal_operand_zone(ctx, left_expr)?.0
	} else if matches!(right_type, DataType::Timestamp(_, _)) {
		temporal_operand_zone(ctx, right_expr)?.0
	} else {
		ctx.session_timezone.to_string()
	};
	crate::timestamp::parse_session_timezone(&zone).map_err(|error| BindingError::TypeMismatch {
		message: error.to_string(),
	})
}

pub(super) fn bind_calendar_arithmetic_operands(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	left_expr: &Expr,
	right_expr: &Expr,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
) -> Result<(BoundExprId, BoundExprId), BindingError> {
	let left = bind_calendar_timestamp_operand(builder, ctx, left_expr, left, left_type)?;
	let right = bind_calendar_timestamp_operand(builder, ctx, right_expr, right, right_type)?;
	Ok((left, right))
}

pub(super) fn bind_calendar_timestamp_operand(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	original: &Expr,
	bound: BoundExprId,
	data_type: &DataType,
) -> Result<BoundExprId, BindingError> {
	if !matches!(data_type, DataType::Timestamp(_, _)) {
		return Ok(bound);
	}
	let unit = calendar_timestamp_arithmetic_unit(data_type, "CalendarMonthInterval arithmetic")?;
	let (zone, _, explicit_zone) = temporal_operand_zone(ctx, original)?;
	if explicit_zone {
		return Ok(bound);
	}
	Ok(builder.append_at_time_zone(
		bound,
		zone.clone(),
		DataType::Timestamp(unit, Some(zone.into())),
	))
}

pub(super) fn validate_temporal_zone(zone: &str) -> Result<String, BindingError> {
	crate::timestamp::parse_session_timezone(zone)
		.map(|zone| zone.to_string())
		.map_err(|error| BindingError::TypeMismatch {
			message: error.to_string(),
		})
}

pub(super) fn supported_timestamp_unit(
	data_type: &DataType,
	operator: &str,
) -> Result<TimeUnit, BindingError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(_),
		) => Ok(*unit),
		DataType::Timestamp(_, None) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp without timezone"),
		}),
		DataType::Timestamp(unit, Some(_)) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp unit {unit:?}"),
		}),
		other => Err(BindingError::TypeMismatch {
			message: format!("{operator} expects Timestamp input, got {other:?}"),
		}),
	}
}

pub(super) fn temporal_arithmetic_output_type(
	op: &BoundBinaryOp,
	left: &DataType,
	right: &DataType,
) -> Result<Option<DataType>, BindingError> {
	let left_is_interval = is_interval_type(left);
	let right_is_interval = is_interval_type(right);
	let left_is_timestamp = matches!(left, DataType::Timestamp(_, _));
	let right_is_timestamp = matches!(right, DataType::Timestamp(_, _));
	let date32_interval = (matches!(left, DataType::Date32) && right_is_interval)
		|| (left_is_interval && matches!(right, DataType::Date32));
	if date32_interval && matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Sub) {
		return Err(BindingError::TypeMismatch {
			message: "Date32 +/- Interval is deferred; use Timestamp arithmetic".to_string(),
		});
	}
	if (left_is_interval || right_is_interval)
		&& matches!(op, BoundBinaryOp::Mul | BoundBinaryOp::Div)
	{
		return Err(BindingError::TypeMismatch {
			message: "Interval multiplication and division are deferred".to_string(),
		});
	}

	match op {
		BoundBinaryOp::Add => match (
			left_is_timestamp,
			right_is_timestamp,
			left_is_interval,
			right_is_interval,
		) {
			(true, true, _, _) => Err(BindingError::TypeMismatch {
				message: "Timestamp + Timestamp is not supported".to_string(),
			}),
			(true, false, false, true) => {
				let unit = timestamp_arithmetic_unit(left, "Timestamp + Interval")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, true, true, false) => {
				let unit = timestamp_arithmetic_unit(right, "Interval + Timestamp")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, false, true, true) => Ok(Some(interval_data_type())),
			(_, _, true, _) | (_, _, _, true) => Err(BindingError::TypeMismatch {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {left:?} and {right:?}"
				),
			}),
			_ => Ok(None),
		},
		BoundBinaryOp::Sub => match (
			left_is_timestamp,
			right_is_timestamp,
			left_is_interval,
			right_is_interval,
		) {
			(true, true, _, _) => {
				let left_unit = timestamp_arithmetic_unit(left, "Timestamp - Timestamp")?;
				let right_unit = timestamp_arithmetic_unit(right, "Timestamp - Timestamp")?;
				if left_unit != right_unit {
					return Err(BindingError::TypeMismatch {
						message: "mixed-unit Timestamp - Timestamp is deferred in ADR 152"
							.to_string(),
					});
				}
				Ok(Some(interval_data_type()))
			}
			(true, false, false, true) => {
				let unit = timestamp_arithmetic_unit(left, "Timestamp - Interval")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, true, true, false) => Err(BindingError::TypeMismatch {
				message: "Interval - Timestamp is not supported".to_string(),
			}),
			(false, false, true, true) => Ok(Some(interval_data_type())),
			(_, _, true, _) | (_, _, _, true) => Err(BindingError::TypeMismatch {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {left:?} and {right:?}"
				),
			}),
			_ => Ok(None),
		},
		BoundBinaryOp::Mul | BoundBinaryOp::Div => Ok(None),
		BoundBinaryOp::Eq
		| BoundBinaryOp::Neq
		| BoundBinaryOp::Lt
		| BoundBinaryOp::Lte
		| BoundBinaryOp::Gt
		| BoundBinaryOp::Gte
		| BoundBinaryOp::And
		| BoundBinaryOp::Or => Ok(None),
	}
}

pub(super) fn calendar_arithmetic_output_type(
	op: &BoundBinaryOp,
	left: &DataType,
	right: &DataType,
) -> Result<Option<DataType>, BindingError> {
	let left_is_calendar = is_calendar_month_interval_type(left);
	let right_is_calendar = is_calendar_month_interval_type(right);
	if !left_is_calendar && !right_is_calendar {
		return Ok(None);
	}
	if (left_is_calendar && is_interval_type(right))
		|| (is_interval_type(left) && right_is_calendar)
	{
		return Err(BindingError::TypeMismatch {
			message:
				"CalendarMonthInterval and fixed-duration Interval arithmetic is not supported"
					.to_string(),
		});
	}
	if matches!(op, BoundBinaryOp::Mul | BoundBinaryOp::Div) {
		return Err(BindingError::TypeMismatch {
			message: "CalendarMonthInterval multiplication and division are deferred".to_string(),
		});
	}

	let left_is_timestamp = matches!(left, DataType::Timestamp(_, _));
	let right_is_timestamp = matches!(right, DataType::Timestamp(_, _));
	let left_is_date32 = matches!(left, DataType::Date32);
	let right_is_date32 = matches!(right, DataType::Date32);

	match op {
		BoundBinaryOp::Add => match (
			left_is_timestamp,
			right_is_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(true, false, false, false, false, true) => {
				let unit =
					calendar_timestamp_arithmetic_unit(left, "Timestamp + CalendarMonthInterval")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, true, false, false, true, false) => {
				let unit =
					calendar_timestamp_arithmetic_unit(right, "CalendarMonthInterval + Timestamp")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, false, true, false, false, true) => Ok(Some(DataType::Date32)),
			(false, false, false, false, true, true) => {
				Ok(Some(calendar_month_interval_data_type()))
			}
			_ => Err(calendar_arithmetic_mismatch(left, right)),
		},
		BoundBinaryOp::Sub => match (
			left_is_timestamp,
			right_is_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(true, false, false, false, false, true) => {
				let unit =
					calendar_timestamp_arithmetic_unit(left, "Timestamp - CalendarMonthInterval")?;
				Ok(Some(DataType::Timestamp(unit, Some("UTC".into()))))
			}
			(false, false, true, false, false, true) => Ok(Some(DataType::Date32)),
			(false, false, false, false, true, true) => {
				Ok(Some(calendar_month_interval_data_type()))
			}
			(false, true, false, false, true, false) => Err(BindingError::TypeMismatch {
				message: "CalendarMonthInterval - Timestamp is not supported".to_string(),
			}),
			_ => Err(calendar_arithmetic_mismatch(left, right)),
		},
		BoundBinaryOp::Mul | BoundBinaryOp::Div => Ok(None),
		BoundBinaryOp::Eq
		| BoundBinaryOp::Neq
		| BoundBinaryOp::Lt
		| BoundBinaryOp::Lte
		| BoundBinaryOp::Gt
		| BoundBinaryOp::Gte
		| BoundBinaryOp::And
		| BoundBinaryOp::Or => Ok(None),
	}
}

pub(super) fn calendar_arithmetic_mismatch(left: &DataType, right: &DataType) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"CalendarMonthInterval arithmetic expects Timestamp, Date32, or CalendarMonthInterval operands, got {left:?} and {right:?}"
		),
	}
}

pub(super) fn is_calendar_month_interval_type(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Interval(IntervalUnit::YearMonth))
}

pub(super) fn is_interval_type(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Duration(TimeUnit::Nanosecond))
}

pub(super) fn calendar_timestamp_arithmetic_unit(
	data_type: &DataType,
	operator: &str,
) -> Result<TimeUnit, BindingError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(_),
		) => Ok(*unit),
		DataType::Timestamp(_, None) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp without timezone"),
		}),
		DataType::Timestamp(unit, Some(_)) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp unit {unit:?}"),
		}),
		other => Err(BindingError::TypeMismatch {
			message: format!("{operator} expects Timestamp input, got {other:?}"),
		}),
	}
}

pub(super) fn timestamp_arithmetic_unit(
	data_type: &DataType,
	operator: &str,
) -> Result<TimeUnit, BindingError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(timezone),
		) if timezone.as_ref() == "UTC" => Ok(*unit),
		DataType::Timestamp(_, None) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp without timezone"),
		}),
		DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() != "UTC" => {
			Err(BindingError::TypeMismatch {
				message: format!("{operator} requires UTC Timestamp, got timezone {timezone}"),
			})
		}
		DataType::Timestamp(unit, Some(_)) => Err(BindingError::TypeMismatch {
			message: format!("{operator} does not support Timestamp unit {unit:?}"),
		}),
		other => Err(BindingError::TypeMismatch {
			message: format!("{operator} expects Timestamp input, got {other:?}"),
		}),
	}
}

pub(super) fn fold_calendar_arithmetic(
	op: &BoundBinaryOp,
	left: &ScalarValue,
	right: &ScalarValue,
	output_type: &DataType,
	timezone: Tz,
) -> Result<ScalarValue, BindingError> {
	if !literal_is_non_null(left) || !literal_is_non_null(right) {
		return typed_null_for_data_type(output_type);
	}
	match (op, left, right) {
		(
			&BoundBinaryOp::Add,
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
			ScalarValue::CalendarMonthInterval(Some(months)),
		) => Ok(ScalarValue::Timestamp {
			value: Some(add_calendar_months_for_bind(
				*timestamp, *unit, timezone, *months,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Add,
			ScalarValue::CalendarMonthInterval(Some(months)),
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
		) => Ok(ScalarValue::Timestamp {
			value: Some(add_calendar_months_for_bind(
				*timestamp, *unit, timezone, *months,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
			ScalarValue::CalendarMonthInterval(Some(months)),
		) => Ok(ScalarValue::Timestamp {
			value: Some(add_calendar_months_for_bind(
				*timestamp,
				*unit,
				timezone,
				negate_calendar_months(*months, op, output_type)?,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Add,
			ScalarValue::Date32(Some(days)),
			ScalarValue::CalendarMonthInterval(Some(months)),
		) => Ok(ScalarValue::Date32(Some(
			add_date32_calendar_months_for_bind(*days, *months)?,
		))),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::Date32(Some(days)),
			ScalarValue::CalendarMonthInterval(Some(months)),
		) => Ok(ScalarValue::Date32(Some(
			add_date32_calendar_months_for_bind(
				*days,
				negate_calendar_months(*months, op, output_type)?,
			)?,
		))),
		(
			&BoundBinaryOp::Add,
			ScalarValue::CalendarMonthInterval(Some(left)),
			ScalarValue::CalendarMonthInterval(Some(right)),
		) => Ok(ScalarValue::CalendarMonthInterval(Some(
			left.checked_add(*right)
				.ok_or_else(|| temporal_arithmetic_overflow(op, output_type))?,
		))),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::CalendarMonthInterval(Some(left)),
			ScalarValue::CalendarMonthInterval(Some(right)),
		) => Ok(ScalarValue::CalendarMonthInterval(Some(
			left.checked_sub(*right)
				.ok_or_else(|| temporal_arithmetic_overflow(op, output_type))?,
		))),
		_ => Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported calendar interval arithmetic literal operands: {left:?} and {right:?}"
			),
		}),
	}
}

pub(super) fn add_calendar_months_for_bind(
	timestamp: i64,
	unit: TimeUnit,
	timezone: Tz,
	months: i32,
) -> Result<i64, BindingError> {
	crate::timestamp::add_calendar_months(timestamp, unit, timezone, months).map_err(|error| {
		BindingError::TypeMismatch {
			message: error.to_string(),
		}
	})
}

pub(super) fn add_date32_calendar_months_for_bind(
	days: i32,
	months: i32,
) -> Result<i32, BindingError> {
	crate::date32::add_calendar_months(days, months).map_err(|error| BindingError::TypeMismatch {
		message: error.to_string(),
	})
}

pub(super) fn negate_calendar_months(
	months: i32,
	op: &BoundBinaryOp,
	output_type: &DataType,
) -> Result<i32, BindingError> {
	months
		.checked_neg()
		.ok_or_else(|| temporal_arithmetic_overflow(op, output_type))
}

pub(super) fn fold_temporal_arithmetic(
	op: &BoundBinaryOp,
	left: &ScalarValue,
	right: &ScalarValue,
	output_type: &DataType,
) -> Result<ScalarValue, BindingError> {
	if !literal_is_non_null(left) || !literal_is_non_null(right) {
		return typed_null_for_data_type(output_type);
	}
	match (op, left, right) {
		(
			&BoundBinaryOp::Add,
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
			ScalarValue::Interval(Some(nanos)),
		) => Ok(ScalarValue::Timestamp {
			value: Some(checked_timestamp_interval_op(
				*timestamp, *nanos, *unit, op,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Add,
			ScalarValue::Interval(Some(nanos)),
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
		) => Ok(ScalarValue::Timestamp {
			value: Some(checked_timestamp_interval_op(
				*timestamp, *nanos, *unit, op,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::Timestamp {
				value: Some(timestamp),
				unit,
			},
			ScalarValue::Interval(Some(nanos)),
		) => Ok(ScalarValue::Timestamp {
			value: Some(checked_timestamp_interval_op(
				*timestamp, *nanos, *unit, op,
			)?),
			unit: *unit,
		}),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::Timestamp {
				value: Some(left),
				unit,
			},
			ScalarValue::Timestamp {
				value: Some(right),
				unit: right_unit,
			},
		) if unit == right_unit => Ok(ScalarValue::Interval(Some(
			checked_timestamp_difference_nanos(*left, *right, *unit)?,
		))),
		(
			&BoundBinaryOp::Add,
			ScalarValue::Interval(Some(left)),
			ScalarValue::Interval(Some(right)),
		) => Ok(ScalarValue::Interval(Some(
			left.checked_add(*right)
				.ok_or_else(|| temporal_arithmetic_overflow(op, output_type))?,
		))),
		(
			&BoundBinaryOp::Sub,
			ScalarValue::Interval(Some(left)),
			ScalarValue::Interval(Some(right)),
		) => Ok(ScalarValue::Interval(Some(
			left.checked_sub(*right)
				.ok_or_else(|| temporal_arithmetic_overflow(op, output_type))?,
		))),
		_ => Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported temporal arithmetic literal operands: {left:?} and {right:?}"
			),
		}),
	}
}

pub(super) fn checked_timestamp_interval_op(
	timestamp: i64,
	interval_nanos: i64,
	unit: TimeUnit,
	op: &BoundBinaryOp,
) -> Result<i64, BindingError> {
	let interval_ticks = checked_interval_nanos_to_timestamp_ticks(interval_nanos, unit)?;
	match op {
		BoundBinaryOp::Add => timestamp.checked_add(interval_ticks),
		BoundBinaryOp::Sub => timestamp.checked_sub(interval_ticks),
		_ => None,
	}
	.ok_or_else(|| temporal_arithmetic_overflow(op, &DataType::Timestamp(unit, Some("UTC".into()))))
}

pub(super) fn checked_timestamp_difference_nanos(
	left: i64,
	right: i64,
	unit: TimeUnit,
) -> Result<i64, BindingError> {
	let diff = left.checked_sub(right).ok_or_else(|| {
		temporal_arithmetic_overflow(
			&BoundBinaryOp::Sub,
			&DataType::Timestamp(unit, Some("UTC".into())),
		)
	})?;
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| BindingError::TypeMismatch {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	diff.checked_mul(nanos_per_unit)
		.ok_or_else(|| temporal_arithmetic_overflow(&BoundBinaryOp::Sub, &interval_data_type()))
}

pub(super) fn checked_interval_nanos_to_timestamp_ticks(
	interval_nanos: i64,
	unit: TimeUnit,
) -> Result<i64, BindingError> {
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| BindingError::TypeMismatch {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	if interval_nanos % nanos_per_unit != 0 {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"interval nanoseconds {interval_nanos} are not exactly representable at {} precision",
				timestamp_precision_name(unit)
			),
		});
	}
	Ok(interval_nanos / nanos_per_unit)
}

pub(super) fn timestamp_precision_name(unit: TimeUnit) -> &'static str {
	match unit {
		TimeUnit::Second => "second",
		TimeUnit::Millisecond => "millisecond",
		TimeUnit::Microsecond => "microsecond",
		TimeUnit::Nanosecond => "nanosecond",
	}
}

pub(super) fn temporal_arithmetic_overflow(
	op: &BoundBinaryOp,
	data_type: &DataType,
) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"temporal arithmetic overflow: {}({data_type:?})",
			bound_arithmetic_name(op)
		),
	}
}

pub(super) fn bound_arithmetic_name(op: &BoundBinaryOp) -> &'static str {
	match op {
		BoundBinaryOp::Add => "add",
		BoundBinaryOp::Sub => "sub",
		BoundBinaryOp::Mul => "mul",
		BoundBinaryOp::Div => "div",
		BoundBinaryOp::Eq
		| BoundBinaryOp::Neq
		| BoundBinaryOp::Lt
		| BoundBinaryOp::Lte
		| BoundBinaryOp::Gt
		| BoundBinaryOp::Gte
		| BoundBinaryOp::And
		| BoundBinaryOp::Or => "non-arithmetic",
	}
}

pub(super) fn bind_logical_ascription(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	logical_type: LogicalTypeId,
) -> Result<BoundExprId, BindingError> {
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let physical = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed logical ascription input");
	validate_logical_ascription_physical(&physical, logical_type, ctx.logical_type_registry)?;
	let existing = builder
		.node_logical_type(inner)
		.expect("builder returns logical type for logical ascription input");
	let logical_type = reconcile_identity(existing, logical_type, &physical)?;
	if !builder.set_node_logical_type(inner, logical_type) {
		return Err(BindingError::UnsupportedExpression {
			message: "logical ascription target is malformed".to_string(),
		});
	}
	Ok(inner)
}

pub(super) fn bind_sql_logical_cast(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	logical_name: &str,
	try_cast: bool,
) -> Result<BoundExprId, BindingError> {
	let spec = ctx.logical_type_specs.get(logical_name).ok_or_else(|| {
		BindingError::UnsupportedExpression {
			message: format!("internal: SQL logical CAST target {logical_name} was not resolved"),
		}
	})?;
	if is_untyped_null(inner) {
		let output = resolve_untyped_null(builder, &spec.physical)?;
		let existing = builder
			.node_logical_type(output)
			.expect("builder returns logical type for SQL logical NULL cast input");
		let logical_type = reconcile_identity(existing, spec.id, &spec.physical)?;
		if !builder.set_node_logical_type(output, logical_type) {
			return Err(BindingError::UnsupportedExpression {
				message: "SQL logical cast target is malformed".to_string(),
			});
		}
		return Ok(output);
	}
	let inner = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let source = builder
		.node_type(inner)
		.cloned()
		.expect("builder returns typed SQL logical cast input");
	let existing = builder
		.node_logical_type(inner)
		.expect("builder returns logical type for SQL logical cast input");
	let logical_type = reconcile_identity(existing, spec.id, &source)?;
	let output = if try_cast {
		if source == spec.physical || is_supported_explicit_numeric_cast(&source, &spec.physical) {
			builder.append_try_cast(inner, spec.physical.clone(), spec.physical.clone())
		} else {
			return Err(BindingError::TypeMismatch {
				message: format!(
					"unsupported TRY_CAST: {source:?} to logical type {}({:?}); only same-physical casts or Int32, Int64, Float32, and Float64 numeric conversions are supported",
					spec.name, spec.physical
				),
			});
		}
	} else if source == spec.physical {
		inner
	} else if is_supported_explicit_numeric_cast(&source, &spec.physical) {
		builder.append_cast(inner, spec.physical.clone(), spec.physical.clone())
	} else {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"unsupported CAST: {source:?} to logical type {}({:?}); only same-physical casts or Int32, Int64, Float32, and Float64 numeric conversions are supported",
				spec.name, spec.physical
			),
		});
	};
	if !builder.set_node_logical_type(output, logical_type) {
		return Err(BindingError::UnsupportedExpression {
			message: "SQL logical cast target is malformed".to_string(),
		});
	}
	Ok(output)
}

pub(super) fn bind_sql_convert(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	inner: &Expr,
	target_logical_name: &str,
) -> Result<BoundExprId, BindingError> {
	let target_spec = ctx
		.logical_type_specs
		.get(target_logical_name)
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!("internal: SQL CONVERT target {target_logical_name} was not resolved"),
		})?;
	let target_unit = target_spec
		.unit
		.as_ref()
		.ok_or_else(|| BindingError::TypeMismatch {
			message: "CONVERT target must be a dimensioned logical type".to_string(),
		})?;
	if target_spec.physical != DataType::Float64 {
		return Err(BindingError::TypeMismatch {
			message: "CONVERT supports Float64 dimensioned logical types only".to_string(),
		});
	}

	let source = bind_expr_into_program(builder, inner, input_schema, ctx)?;
	let source_type = builder
		.node_type(source)
		.cloned()
		.expect("builder returns typed CONVERT source");
	let source_logical = builder
		.node_logical_type(source)
		.expect("builder returns logical type for CONVERT source");
	let source_spec = LogicalTypeResolver::with_registry(ctx.logical_type_registry)
		.spec(source_logical)
		.ok_or_else(|| BindingError::TypeMismatch {
			message: "CONVERT source must be a dimensioned logical type".to_string(),
		})?;
	let source_unit = source_spec
		.unit
		.as_ref()
		.ok_or_else(|| BindingError::TypeMismatch {
			message: "CONVERT source must be a dimensioned logical type".to_string(),
		})?;
	if source_type != DataType::Float64 || source_spec.physical != DataType::Float64 {
		return Err(BindingError::TypeMismatch {
			message: "CONVERT supports Float64 dimensioned logical types only".to_string(),
		});
	}
	if source_unit.dimension != target_unit.dimension {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"CONVERT dimension mismatch: source {} is {}, target {} is {}",
				source_spec.name, source_unit.dimension, target_spec.name, target_unit.dimension
			),
		});
	}
	if source_unit.quantity_kind != target_unit.quantity_kind {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"CONVERT point/delta mismatch: source {} is {:?}, target {} is {:?}",
				source_spec.name,
				source_unit.quantity_kind,
				target_spec.name,
				target_unit.quantity_kind
			),
		});
	}
	let transform = logical_unit_convert_transform(source_unit, target_unit);
	if !transform.factor.is_finite() || transform.factor <= 0.0 {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"CONVERT scale from {} to {} is invalid",
				source_spec.name, target_spec.name
			),
		});
	}
	if !transform.shift.is_finite() {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"CONVERT affine shift from {} to {} is invalid",
				source_spec.name, target_spec.name
			),
		});
	}
	let factor = builder.append_literal(ScalarValue::Float64(Some(transform.factor)));
	let mut output = builder.append_binary(BoundBinaryOp::Mul, source, factor, DataType::Float64);
	if transform.shift != 0.0 {
		let shift = builder.append_literal(ScalarValue::Float64(Some(transform.shift)));
		output = builder.append_binary(BoundBinaryOp::Add, output, shift, DataType::Float64);
	}
	if !builder.set_node_logical_type(output, target_spec.id) {
		return Err(BindingError::UnsupportedExpression {
			message: "CONVERT target logical identity is malformed".to_string(),
		});
	}
	Ok(output)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LogicalUnitConvertTransform {
	pub(super) factor: f64,
	pub(super) shift: f64,
}

pub(super) fn logical_unit_convert_transform(
	source_unit: &LogicalTypeUnit,
	target_unit: &LogicalTypeUnit,
) -> LogicalUnitConvertTransform {
	let factor = unit_scale_to_f64(&source_unit.scale_to_base)
		/ unit_scale_to_f64(&target_unit.scale_to_base);
	let shift = if source_unit.quantity_kind == UnitQuantityKind::Point {
		(unit_offset_to_f64(&source_unit.offset_to_base)
			- unit_offset_to_f64(&target_unit.offset_to_base))
			/ unit_scale_to_f64(&target_unit.scale_to_base)
	} else {
		0.0
	};
	LogicalUnitConvertTransform { factor, shift }
}

pub(super) fn unit_scale_to_f64(scale: &UnitScale) -> f64 {
	scale.numerator as f64 / scale.denominator as f64
}

pub(super) fn unit_offset_to_f64(offset: &UnitOffset) -> f64 {
	offset.numerator as f64 / offset.denominator as f64
}

pub(super) fn validate_logical_ascription_physical(
	physical: &DataType,
	logical_type: LogicalTypeId,
	registry: &LogicalTypeRegistry,
) -> Result<(), BindingError> {
	if logical_type.is_naked() {
		return Ok(());
	}
	// ADR 265: resolve through the catalog registry (seed-or-durable), not the seed-only free fn, so a
	// durable runtime-registered logical type on a join key / ascription resolves. No seed-only fallback
	// here — that would let a missing/empty registry "succeed" via seeds and mask a threading bug; the
	// non-seed positive tests prove the registry is actually threaded.
	let Some(spec) = LogicalTypeResolver::with_registry(registry).spec(logical_type) else {
		return Err(BindingError::UnsupportedExpression {
			message: format!("unknown logical type {}", logical_type_name(logical_type)),
		});
	};
	if spec.physical != *physical {
		return Err(BindingError::TypeMismatch {
			message: format!(
				"logical type {} expects {:?}, got {physical:?}",
				spec.name, spec.physical
			),
		});
	}
	Ok(())
}

pub(super) fn bind_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	input_schema: &SchemaRef,
	ctx: &ExprBindContext<'_>,
	left: &Expr,
	right: &Expr,
	op: BoundBinaryOp,
) -> Result<BoundExprId, BindingError> {
	match (is_untyped_null(left), is_untyped_null(right)) {
		(true, true) => {
			return Err(BindingError::TypeMismatch {
				message: "arithmetic with NULL requires a typed operand".to_string(),
			});
		}
		(true, false) => {
			let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
			let right_type = builder
				.node_type(right)
				.cloned()
				.expect("builder returns typed right node");
			if matches!(
				op,
				BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div
			) {
				reject_logical_arithmetic_with_untyped_null(builder, ctx, &op, right)?;
			}
			let output_type =
				arithmetic_output_type(&right_type, &right_type).ok_or_else(|| {
					BindingError::TypeMismatch {
						message: format!(
							"arithmetic operands must be Int32, Int64, Float32, or Float64, got NULL and {right_type:?}"
						),
					}
				})?;
			let left = resolve_untyped_null(builder, &output_type)?;
			let right = cast_if_needed(builder, right, &output_type)?;
			return Ok(builder.append_binary(op, left, right, output_type));
		}
		(false, true) => {
			let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
			let left_type = builder
				.node_type(left)
				.cloned()
				.expect("builder returns typed left node");
			if matches!(
				op,
				BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div
			) {
				reject_logical_arithmetic_with_untyped_null(builder, ctx, &op, left)?;
			}
			let output_type = arithmetic_output_type(&left_type, &left_type).ok_or_else(|| {
				BindingError::TypeMismatch {
					message: format!(
						"arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and NULL"
					),
				}
			})?;
			let left = cast_if_needed(builder, left, &output_type)?;
			let right = resolve_untyped_null(builder, &output_type)?;
			return Ok(builder.append_binary(op, left, right, output_type));
		}
		(false, false) => {}
	}
	let left_expr = left;
	let right_expr = right;
	let left = bind_expr_into_program(builder, left, input_schema, ctx)?;
	let right = bind_expr_into_program(builder, right, input_schema, ctx)?;
	let left_type = builder
		.node_type(left)
		.cloned()
		.expect("builder returns typed left node");
	let right_type = builder
		.node_type(right)
		.cloned()
		.expect("builder returns typed right node");
	// ADR 266: coerce an Int/Float literal operand to the other side's Decimal128(p,s) so `+`/`-` over a
	// decimal column and a numeric literal (`1 - l_discount`) binds — rewrite in place (no Cast node),
	// mirroring the comparison coercion. Gated to `+`/`-`, so non-decimal arithmetic is untouched.
	let (left_type, right_type) = if matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Sub) {
		coerce_decimal_arithmetic_literal(builder, ctx, left, right, &left_type, &right_type)?;
		(
			builder
				.node_type(left)
				.cloned()
				.expect("builder returns typed left node"),
			builder
				.node_type(right)
				.cloned()
				.expect("builder returns typed right node"),
		)
	} else {
		(left_type, right_type)
	};
	if let Some(output_type) = calendar_arithmetic_output_type(&op, &left_type, &right_type)? {
		if let (Some(left_literal), Some(right_literal)) = (
			builder.literal(left).cloned(),
			builder.literal(right).cloned(),
		) {
			let timezone =
				calendar_arithmetic_timezone(ctx, left_expr, right_expr, &left_type, &right_type)?;
			let folded = fold_calendar_arithmetic(
				&op,
				&left_literal,
				&right_literal,
				&output_type,
				timezone,
			)?;
			return Ok(builder.append_literal(folded));
		}
		let (left, right) = bind_calendar_arithmetic_operands(
			builder,
			ctx,
			left_expr,
			right_expr,
			left,
			right,
			&left_type,
			&right_type,
		)?;
		return Ok(builder.append_binary(op, left, right, output_type));
	}
	if let Some(output_type) = temporal_arithmetic_output_type(&op, &left_type, &right_type)? {
		if let (Some(left_literal), Some(right_literal)) = (
			builder.literal(left).cloned(),
			builder.literal(right).cloned(),
		) {
			let folded =
				fold_temporal_arithmetic(&op, &left_literal, &right_literal, &output_type)?;
			return Ok(builder.append_literal(folded));
		}
		return Ok(builder.append_binary(op, left, right, output_type));
	}
	if matches!(
		op,
		BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div
	) && let Some(output) =
		bind_logical_arithmetic(builder, ctx, &op, left, right, &left_type, &right_type)?
	{
		return Ok(output);
	}
	if let Some(output) =
		bind_dimensioned_arithmetic(builder, ctx, left, right, &left_type, &right_type, &op)?
	{
		return Ok(output);
	}
	// ADR 263: Decimal128 * Decimal128 -> Decimal128(p1+p2+1, s1+s2) (the SQL mul rule). Operands keep
	// their own (p,s); the executor routes to Arrow `numeric::mul`, which widens. Q6 slice — only `*`;
	// decimal +/-/÷ fall through to the unsupported error below until a query needs them.
	if op == BoundBinaryOp::Mul
		&& let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) =
			(&left_type, &right_type)
	{
		let precision = (p1 + p2 + 1).min(38);
		let scale = s1 + s2;
		return Ok(builder.append_binary(op, left, right, DataType::Decimal128(precision, scale)));
	}
	// ADR 266: same-(p,s) Decimal128 +/- Decimal128 -> Decimal128(min(38, p+1), s) — carry one precision
	// digit, scale unchanged (Arrow `numeric::add`/`sub` matches; verified by arrow_decimal_probe). Q1
	// slice — only `+`/`-` at equal (p,s); mixed-scale/precision +/- and `÷` stay unsupported (no Decimal
	// cast surface) until a query needs them.
	if matches!(op, BoundBinaryOp::Add | BoundBinaryOp::Sub)
		&& let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) =
			(&left_type, &right_type)
		&& p1 == p2
		&& s1 == s2
	{
		let precision = (p1 + 1).min(38);
		return Ok(builder.append_binary(op, left, right, DataType::Decimal128(precision, *s1)));
	}
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

/// ADR 266: coerce an Int/Float literal operand to the other operand's `Decimal128(p,s)` (rewrite in
/// place) so decimal `+`/`-` with a numeric literal binds. No-op unless one side is `Decimal128` and the
/// other an Int/Float literal; reuses the checked `coerce_literal_value` path (no unchecked scaling).
fn coerce_decimal_arithmetic_literal(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
) -> Result<(), BindingError> {
	let is_numeric_literal = |value: &ScalarValue| {
		matches!(
			value,
			ScalarValue::Int32(_)
				| ScalarValue::Int64(_)
				| ScalarValue::Float32(_)
				| ScalarValue::Float64(_)
		)
	};
	if matches!(right_type, DataType::Decimal128(_, _))
		&& let Some(value) = builder.literal(left).cloned()
		&& is_numeric_literal(&value)
	{
		let value = coerce_literal_value(value, right_type, ctx.session_timezone)?;
		let _ = builder.rewrite_literal(left, value);
	} else if matches!(left_type, DataType::Decimal128(_, _))
		&& let Some(value) = builder.literal(right).cloned()
		&& is_numeric_literal(&value)
	{
		let value = coerce_literal_value(value, left_type, ctx.session_timezone)?;
		let _ = builder.rewrite_literal(right, value);
	}
	Ok(())
}

pub(super) enum LogicalArithmeticDomain {
	Naked,
	Quantity(LogicalTypeSpec),
	Identity(LogicalTypeSpec),
}

pub(super) fn logical_arithmetic_domain_for_node(
	ctx: &ExprBindContext<'_>,
	builder: &BoundExprProgramBuilder,
	id: BoundExprId,
) -> Result<LogicalArithmeticDomain, BindingError> {
	let logical_type = builder
		.node_logical_type(id)
		.expect("builder returns logical type for arithmetic domain lookup");
	logical_arithmetic_domain_for_type(logical_type, ctx.logical_type_registry)
}

pub(super) fn logical_arithmetic_domain_for_type(
	logical_type: LogicalTypeId,
	logical_types: &LogicalTypeRegistry,
) -> Result<LogicalArithmeticDomain, BindingError> {
	if logical_type.is_naked() {
		return Ok(LogicalArithmeticDomain::Naked);
	}
	let spec = LogicalTypeResolver::with_registry(logical_types)
		.spec(logical_type)
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!("unknown logical type {}", logical_type_name(logical_type)),
		})?;
	if spec.unit.is_some() {
		Ok(LogicalArithmeticDomain::Quantity(spec))
	} else {
		Ok(LogicalArithmeticDomain::Identity(spec))
	}
}

pub(super) fn bind_logical_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	op: &BoundBinaryOp,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
) -> Result<Option<BoundExprId>, BindingError> {
	let left_logical = builder
		.node_logical_type(left)
		.expect("builder returns logical type for arithmetic left");
	let left_domain = logical_arithmetic_domain_for_node(ctx, builder, left)?;
	let right_domain = logical_arithmetic_domain_for_node(ctx, builder, right)?;
	if let LogicalArithmeticDomain::Identity(spec) = &left_domain {
		return Err(non_arithmetic_logical_type_error(
			&spec.name,
			arithmetic_symbol(op),
		));
	}
	if let LogicalArithmeticDomain::Identity(spec) = &right_domain {
		return Err(non_arithmetic_logical_type_error(
			&spec.name,
			arithmetic_symbol(op),
		));
	}
	if domain_is_point(&left_domain) || domain_is_point(&right_domain) {
		return bind_point_delta_arithmetic(
			builder,
			ctx,
			op,
			left,
			right,
			left_type,
			right_type,
			&left_domain,
			&right_domain,
		);
	}
	if let LogicalArithmeticDomain::Quantity(spec) = &left_domain
		&& quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Point)
	{
		return Err(point_arithmetic_error(spec, arithmetic_symbol(op)));
	}
	if let LogicalArithmeticDomain::Quantity(spec) = &right_domain
		&& quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Point)
	{
		return Err(point_arithmetic_error(spec, arithmetic_symbol(op)));
	}
	if !matches!(op, BoundBinaryOp::Add) {
		return Ok(None);
	}
	match (left_domain, right_domain) {
		(LogicalArithmeticDomain::Naked, LogicalArithmeticDomain::Naked) => Ok(None),
		(
			LogicalArithmeticDomain::Quantity(left_unit),
			LogicalArithmeticDomain::Quantity(right_unit),
		) => {
			require_compatible_units(&left_unit, &right_unit)?;
			Ok(Some(append_dimensioned_arithmetic(
				builder,
				BoundBinaryOp::Add,
				left,
				right,
				left_type,
				right_type,
				left_logical,
			)?))
		}
		(LogicalArithmeticDomain::Quantity(_), LogicalArithmeticDomain::Naked)
		| (LogicalArithmeticDomain::Naked, LogicalArithmeticDomain::Quantity(_)) => {
			Err(unit_type_mismatch(
				"unit arithmetic requires both operands to be the same unit; CONVERT explicitly before combining with a scalar",
			))
		}
		(LogicalArithmeticDomain::Identity(_), _) | (_, LogicalArithmeticDomain::Identity(_)) => {
			unreachable!("identity operands handled before operator-specific arithmetic")
		}
	}
}

pub(super) fn bind_point_delta_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	op: &BoundBinaryOp,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
	left_domain: &LogicalArithmeticDomain,
	right_domain: &LogicalArithmeticDomain,
) -> Result<Option<BoundExprId>, BindingError> {
	match (left_domain, right_domain) {
		(
			LogicalArithmeticDomain::Quantity(left_spec),
			LogicalArithmeticDomain::Quantity(right_spec),
		) => bind_point_delta_quantity_arithmetic(
			builder, ctx, op, left, right, left_type, right_type, left_spec, right_spec,
		),
		(LogicalArithmeticDomain::Quantity(spec), LogicalArithmeticDomain::Naked)
		| (LogicalArithmeticDomain::Naked, LogicalArithmeticDomain::Quantity(spec))
			if is_point_quantity(spec) =>
		{
			Err(point_arithmetic_error(spec, arithmetic_symbol(op)))
		}
		_ => Ok(None),
	}
}

pub(super) fn bind_point_delta_quantity_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	op: &BoundBinaryOp,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
	left_spec: &LogicalTypeSpec,
	right_spec: &LogicalTypeSpec,
) -> Result<Option<BoundExprId>, BindingError> {
	let left_is_point = is_point_quantity(left_spec);
	let right_is_point = is_point_quantity(right_spec);
	if !left_is_point && !right_is_point {
		return Ok(None);
	}
	match (left_is_point, right_is_point, op) {
		(true, true, BoundBinaryOp::Sub) => {
			if left_spec.id != right_spec.id {
				return Err(unit_type_mismatch(format!(
					"point subtraction requires both operands to be the same point unit; CONVERT explicitly before subtracting {} and {}",
					left_spec.name, right_spec.name
				)));
			}
			let delta_spec = paired_delta_spec(left_spec, ctx.logical_type_registry)?;
			Ok(Some(append_dimensioned_arithmetic(
				builder,
				BoundBinaryOp::Sub,
				left,
				right,
				left_type,
				right_type,
				delta_spec.id,
			)?))
		}
		(true, true, BoundBinaryOp::Add) => Err(point_arithmetic_error(left_spec, "point + point")),
		(true, false, BoundBinaryOp::Add | BoundBinaryOp::Sub) => {
			require_declared_delta_unit(left_spec, right_spec, ctx.logical_type_registry)?;
			Ok(Some(append_dimensioned_arithmetic(
				builder,
				op.clone(),
				left,
				right,
				left_type,
				right_type,
				left_spec.id,
			)?))
		}
		(false, true, BoundBinaryOp::Add) => {
			require_declared_delta_unit(right_spec, left_spec, ctx.logical_type_registry)?;
			Ok(Some(append_dimensioned_arithmetic(
				builder,
				BoundBinaryOp::Add,
				left,
				right,
				left_type,
				right_type,
				right_spec.id,
			)?))
		}
		(false, true, BoundBinaryOp::Sub) => Err(unit_type_mismatch(format!(
			"delta minus point is not supported; subtract a declared delta unit from point {} instead",
			right_spec.name
		))),
		(true, _, BoundBinaryOp::Mul | BoundBinaryOp::Div)
		| (_, true, BoundBinaryOp::Mul | BoundBinaryOp::Div) => Err(point_arithmetic_error(
			if left_is_point { left_spec } else { right_spec },
			arithmetic_symbol(op),
		)),
		(true, _, _) | (_, true, _) => Err(point_arithmetic_error(
			if left_is_point { left_spec } else { right_spec },
			arithmetic_symbol(op),
		)),
		(false, false, _) => Ok(None),
	}
}

pub(super) fn reject_logical_arithmetic_with_untyped_null(
	builder: &BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	op: &BoundBinaryOp,
	typed_operand: BoundExprId,
) -> Result<(), BindingError> {
	match logical_arithmetic_domain_for_node(ctx, builder, typed_operand)? {
		LogicalArithmeticDomain::Naked => Ok(()),
		LogicalArithmeticDomain::Quantity(spec) => {
			if quantity_kind_for_spec(&spec) == Some(UnitQuantityKind::Point) {
				Err(point_arithmetic_error(&spec, arithmetic_symbol(op)))
			} else {
				Err(unit_type_mismatch(
					"unit arithmetic with untyped NULL requires an explicit typed NULL or primitive CAST",
				))
			}
		}
		LogicalArithmeticDomain::Identity(spec) => Err(non_arithmetic_logical_type_error(
			&spec.name,
			arithmetic_symbol(op),
		)),
	}
}

pub(super) fn bind_dimensioned_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
	op: &BoundBinaryOp,
) -> Result<Option<BoundExprId>, BindingError> {
	let left_logical = builder
		.node_logical_type(left)
		.expect("builder returns logical type for arithmetic left");
	let right_logical = builder
		.node_logical_type(right)
		.expect("builder returns logical type for arithmetic right");
	let left_unit = logical_unit_for_node(ctx, builder, left)?;
	let right_unit = logical_unit_for_node(ctx, builder, right)?;
	if left_unit.is_none() && right_unit.is_none() {
		return Ok(None);
	}
	if let Some(spec) = left_unit.as_ref()
		&& quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Point)
	{
		return Err(point_arithmetic_error(spec, arithmetic_symbol(op)));
	}
	if let Some(spec) = right_unit.as_ref()
		&& quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Point)
	{
		return Err(point_arithmetic_error(spec, arithmetic_symbol(op)));
	}

	match op {
		BoundBinaryOp::Add | BoundBinaryOp::Sub => {
			let (Some(left_unit), Some(right_unit)) = (left_unit, right_unit) else {
				return Err(unit_type_mismatch(
					"unit arithmetic requires both operands to be the same unit; CONVERT explicitly before combining with a scalar",
				));
			};
			require_compatible_units(&left_unit, &right_unit)?;
			let output = append_dimensioned_arithmetic(
				builder,
				op.clone(),
				left,
				right,
				left_type,
				right_type,
				left_logical,
			)?;
			Ok(Some(output))
		}
		BoundBinaryOp::Mul => match (left_unit, right_unit) {
			(Some(_), Some(_)) => Err(unit_type_mismatch(
				"unit multiplication that creates derived dimensions is deferred",
			)),
			(Some(_), None) if right_logical.is_naked() => Ok(Some(append_dimensioned_arithmetic(
				builder,
				op.clone(),
				left,
				right,
				left_type,
				right_type,
				left_logical,
			)?)),
			(None, Some(_)) if left_logical.is_naked() => Ok(Some(append_dimensioned_arithmetic(
				builder,
				op.clone(),
				left,
				right,
				left_type,
				right_type,
				right_logical,
			)?)),
			_ => Err(unit_type_mismatch(
				"unit multiplication supports only one dimensioned operand and one naked numeric operand",
			)),
		},
		BoundBinaryOp::Div => match (left_unit, right_unit) {
			(Some(_), Some(_)) => Err(unit_type_mismatch(
				"unit division that creates ratios is deferred",
			)),
			(Some(_), None) if right_logical.is_naked() => Ok(Some(append_dimensioned_arithmetic(
				builder,
				op.clone(),
				left,
				right,
				left_type,
				right_type,
				left_logical,
			)?)),
			(None, Some(_)) => Err(unit_type_mismatch(
				"naked numeric divided by a unit is deferred",
			)),
			_ => Err(unit_type_mismatch(
				"unit division supports only a dimensioned numerator and naked numeric denominator",
			)),
		},
		_ => Ok(None),
	}
}

pub(super) fn append_dimensioned_arithmetic(
	builder: &mut BoundExprProgramBuilder,
	op: BoundBinaryOp,
	left: BoundExprId,
	right: BoundExprId,
	left_type: &DataType,
	right_type: &DataType,
	output_logical: LogicalTypeId,
) -> Result<BoundExprId, BindingError> {
	let output_type =
		arithmetic_output_type(left_type, right_type).ok_or_else(|| BindingError::TypeMismatch {
			message: format!(
				"unit arithmetic operands must be Int32, Int64, Float32, or Float64, got {left_type:?} and {right_type:?}"
			),
		})?;
	let left = cast_if_needed(builder, left, &output_type)?;
	let right = cast_if_needed(builder, right, &output_type)?;
	let output = builder.append_binary(op, left, right, output_type);
	if !builder.set_node_logical_type(output, output_logical) {
		return Err(BindingError::UnsupportedExpression {
			message: "unit arithmetic logical identity target is malformed".to_string(),
		});
	}
	Ok(output)
}

pub(super) fn bind_dimensioned_comparison(
	builder: &mut BoundExprProgramBuilder,
	ctx: &ExprBindContext<'_>,
	left: BoundExprId,
	right: BoundExprId,
	op: &BoundBinaryOp,
) -> Result<Option<BoundExprId>, BindingError> {
	let left_unit = logical_unit_for_node(ctx, builder, left)?;
	let right_unit = logical_unit_for_node(ctx, builder, right)?;
	if left_unit.is_none() && right_unit.is_none() {
		return Ok(None);
	}
	let (Some(left_unit), Some(right_unit)) = (left_unit, right_unit) else {
		return Err(unit_type_mismatch(
			"unit comparison requires both operands to be the same unit; CONVERT explicitly",
		));
	};
	require_compatible_units(&left_unit, &right_unit)?;
	Ok(Some(builder.append_binary(
		op.clone(),
		left,
		right,
		DataType::Boolean,
	)))
}

pub(super) fn logical_unit_for_node(
	ctx: &ExprBindContext<'_>,
	builder: &BoundExprProgramBuilder,
	id: BoundExprId,
) -> Result<Option<LogicalTypeSpec>, BindingError> {
	let logical_type = builder
		.node_logical_type(id)
		.expect("builder returns logical type for unit lookup");
	if logical_type.is_naked() {
		return Ok(None);
	}
	let spec = LogicalTypeResolver::with_registry(ctx.logical_type_registry)
		.spec(logical_type)
		.ok_or_else(|| BindingError::UnsupportedExpression {
			message: format!("unknown logical type {}", logical_type_name(logical_type)),
		})?;
	if spec.unit.is_some() {
		Ok(Some(spec))
	} else {
		Ok(None)
	}
}

pub(super) fn require_compatible_units(
	left: &LogicalTypeSpec,
	right: &LogicalTypeSpec,
) -> Result<(), BindingError> {
	if left.id == right.id {
		return Ok(());
	}
	let left_unit = left
		.unit
		.as_ref()
		.expect("caller passes dimensioned left logical type");
	let right_unit = right
		.unit
		.as_ref()
		.expect("caller passes dimensioned right logical type");
	if left_unit.dimension == right_unit.dimension {
		return Err(unit_type_mismatch(format!(
			"same-dimension units {} and {} require explicit CONVERT",
			left.name, right.name
		)));
	}
	Err(unit_type_mismatch(format!(
		"dimension mismatch: cannot combine {} ({}) and {} ({})",
		left.name, left_unit.dimension, right.name, right_unit.dimension
	)))
}

pub(super) fn unit_type_mismatch(message: impl Into<String>) -> BindingError {
	BindingError::TypeMismatch {
		message: message.into(),
	}
}

pub(super) fn domain_is_point(domain: &LogicalArithmeticDomain) -> bool {
	match domain {
		LogicalArithmeticDomain::Quantity(spec) => is_point_quantity(spec),
		LogicalArithmeticDomain::Naked | LogicalArithmeticDomain::Identity(_) => false,
	}
}

pub(super) fn quantity_kind_for_spec(spec: &LogicalTypeSpec) -> Option<UnitQuantityKind> {
	spec.unit.as_ref().map(|unit| unit.quantity_kind)
}

pub(super) fn is_point_quantity(spec: &LogicalTypeSpec) -> bool {
	quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Point)
}

pub(super) fn is_delta_quantity(spec: &LogicalTypeSpec) -> bool {
	quantity_kind_for_spec(spec) == Some(UnitQuantityKind::Delta)
}

pub(super) fn paired_delta_spec(
	point: &LogicalTypeSpec,
	logical_types: &LogicalTypeRegistry,
) -> Result<LogicalTypeSpec, BindingError> {
	let Some(point_unit) = point.unit.as_ref() else {
		return Err(unit_type_mismatch(format!(
			"point type {} is not dimensioned",
			point.name
		)));
	};
	let Some(delta_name) = point_unit.delta_unit.as_deref() else {
		return Err(unit_type_mismatch(format!(
			"point type {} has no declared delta unit",
			point.name
		)));
	};
	let delta = LogicalTypeResolver::with_registry(logical_types)
		.spec_by_name(delta_name)
		.ok_or_else(|| {
			unit_type_mismatch(format!(
				"declared delta unit {delta_name} for point type {} was not found",
				point.name
			))
		})?;
	validate_declared_delta_unit(point, &delta)?;
	Ok(delta)
}

pub(super) fn require_declared_delta_unit(
	point: &LogicalTypeSpec,
	delta: &LogicalTypeSpec,
	logical_types: &LogicalTypeRegistry,
) -> Result<(), BindingError> {
	let paired = paired_delta_spec(point, logical_types)?;
	if paired.id != delta.id {
		return Err(unit_type_mismatch(format!(
			"delta unit {} is not the declared delta unit for point type {}; CONVERT explicitly",
			delta.name, point.name
		)));
	}
	Ok(())
}

pub(super) fn validate_declared_delta_unit(
	point: &LogicalTypeSpec,
	delta: &LogicalTypeSpec,
) -> Result<(), BindingError> {
	if !is_delta_quantity(delta) {
		return Err(unit_type_mismatch(format!(
			"declared delta unit {} for point type {} must be a Delta unit",
			delta.name, point.name
		)));
	}
	if point.physical != delta.physical {
		return Err(unit_type_mismatch(format!(
			"declared delta unit {} physical type must match point type {}",
			delta.name, point.name
		)));
	}
	let point_unit = point
		.unit
		.as_ref()
		.expect("caller validates point quantity");
	let delta_unit = delta
		.unit
		.as_ref()
		.expect("delta quantity has unit metadata");
	if point_unit.dimension != delta_unit.dimension {
		return Err(unit_type_mismatch(format!(
			"declared delta unit {} dimension must match point type {}",
			delta.name, point.name
		)));
	}
	if point_unit.scale_to_base != delta_unit.scale_to_base {
		return Err(unit_type_mismatch(format!(
			"declared delta unit {} scale must match point type {}",
			delta.name, point.name
		)));
	}
	Ok(())
}

pub(super) fn point_arithmetic_error(spec: &LogicalTypeSpec, context: &str) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"point quantity {} does not support arithmetic for {context}; point/delta quantity algebra allows only operations with the declared delta unit, use an explicit primitive CAST for raw math",
			spec.name
		),
	}
}

pub(super) fn arithmetic_symbol(op: &BoundBinaryOp) -> &'static str {
	match op {
		BoundBinaryOp::Add => "+",
		BoundBinaryOp::Sub => "-",
		BoundBinaryOp::Mul => "*",
		BoundBinaryOp::Div => "/",
		BoundBinaryOp::Eq
		| BoundBinaryOp::Neq
		| BoundBinaryOp::Lt
		| BoundBinaryOp::Lte
		| BoundBinaryOp::Gt
		| BoundBinaryOp::Gte
		| BoundBinaryOp::And
		| BoundBinaryOp::Or => "arithmetic",
	}
}

pub(super) fn non_arithmetic_logical_type_error(type_name: &str, context: &str) -> BindingError {
	BindingError::TypeMismatch {
		message: format!(
			"logical type {type_name} is not an arithmetic quantity for {context}; use an explicit primitive CAST to perform raw numeric arithmetic"
		),
	}
}
