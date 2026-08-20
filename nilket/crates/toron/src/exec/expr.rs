use super::*;

#[derive(Debug, Clone, Copy)]
pub(in crate::exec) enum ArithmeticOp {
	Add,
	Sub,
	Mul,
	Div,
}

impl ArithmeticOp {
	fn name(self) -> &'static str {
		match self {
			Self::Add => "Add",
			Self::Sub => "Sub",
			Self::Mul => "Mul",
			Self::Div => "Div",
		}
	}
}

const F64_I32_LOWER: f64 = i32::MIN as f64;
const F64_I32_UPPER: f64 = 2147483648.0;
const F64_I64_LOWER: f64 = i64::MIN as f64;
const F64_I64_UPPER: f64 = 9223372036854775808.0;

impl Executor {
	pub(crate) fn filter_batches_with_bound_predicate(
		&self,
		predicate: &BoundExprProgram,
		batches: Vec<RecordBatch>,
		context: &'static str,
	) -> Result<Vec<RecordBatch>, ExecutionError> {
		let mut output = Vec::with_capacity(batches.len());
		for batch in batches {
			let mask = self.evaluate_bound_single_root(predicate, &batch, context)?;
			let mask_bool = mask
				.as_any()
				.downcast_ref::<BooleanArray>()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: format!("{context} must return boolean"),
				})?;
			output.push(filter_record_batch(&batch, mask_bool)?);
		}
		Ok(output)
	}

	pub(in crate::exec) fn filter_batch_with_bound_predicate(
		&self,
		predicate: &BoundExprProgram,
		batch: RecordBatch,
		context: &'static str,
	) -> Result<RecordBatch, ExecutionError> {
		let mask = self.evaluate_bound_single_root(predicate, &batch, context)?;
		let mask_bool = mask
			.as_any()
			.downcast_ref::<BooleanArray>()
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!("{context} must return boolean"),
			})?;
		Ok(filter_record_batch(&batch, mask_bool)?)
	}

	#[cfg(test)]
	pub fn evaluate_expr(
		&self,
		expr: &Expr,
		batch: &RecordBatch,
	) -> Result<ArrayRef, ExecutionError> {
		match expr {
			Expr::Column(column_ref) => {
				let ordinal = batch.schema().index_of(&column_ref.name.0).map_err(|_| {
					ExecutionError::ColumnNotFound {
						name: column_ref.name.0.clone(),
					}
				})?;
				Ok(batch.column(ordinal).clone())
			}
			Expr::Literal(val) => {
				let num_rows = batch.num_rows();
				match val {
					ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
					ScalarValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
					ScalarValue::Int64(v) => {
						Ok(Arc::new(arrow::array::Int64Array::from(vec![*v; num_rows])))
					}
					ScalarValue::Date32(v) => Ok(Arc::new(Date32Array::from(vec![*v; num_rows]))),
					ScalarValue::Timestamp { value, unit } => {
						Ok(timestamp_literal_array(value, *unit, num_rows))
					}
					ScalarValue::Interval(value) => {
						Ok(Arc::new(DurationNanosecondArray::from(vec![
							*value;
							num_rows
						])))
					}
					ScalarValue::CalendarMonthInterval(value) => {
						Ok(Arc::new(IntervalYearMonthArray::from(vec![
							*value;
							num_rows
						])))
					}
					ScalarValue::Float32(v) => {
						Ok(Arc::new(arrow::array::Float32Array::from(vec![
							*v;
							num_rows
						])))
					}
					ScalarValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; num_rows]))),
					ScalarValue::Decimal128 {
						value,
						precision,
						scale,
					} => Ok(Arc::new(
						Decimal128Array::from(vec![*value; num_rows])
							.with_precision_and_scale(*precision, *scale)
							.map_err(|error| ExecutionError::Kernel {
								message: error.to_string(),
							})?,
					)),
					ScalarValue::Utf8(v) => Ok(Arc::new(StringArray::from(vec![
						v.clone()
							.as_deref();
						num_rows
					]))),
				}
			}
			Expr::UntypedNull => Err(ExecutionError::Kernel {
				message: "internal: unresolved NULL literal reached execution".to_string(),
			}),
			Expr::Cast(inner, target) => {
				let arr = self.evaluate_expr(inner, batch)?;
				evaluate_cast(&arr, target)
			}
			Expr::TryCast(inner, target) => {
				let arr = self.evaluate_expr(inner, batch)?;
				evaluate_try_cast(&arr, target)
			}
			Expr::LogicalAscription { expr, .. } => self.evaluate_expr(expr, batch),
			Expr::SqlLogicalCast { .. } => Err(ExecutionError::Kernel {
				message: "internal: unresolved SQL logical CAST reached execution".to_string(),
			}),
			Expr::SqlConvert { .. } => Err(ExecutionError::Kernel {
				message: "internal: unresolved SQL CONVERT reached execution".to_string(),
			}),
			Expr::Add(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Add, left_arr, right_arr)
			}
			Expr::Sub(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Sub, left_arr, right_arr)
			}
			Expr::Mul(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Mul, left_arr, right_arr)
			}
			Expr::Div(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				evaluate_unbound_arithmetic(ArithmeticOp::Div, left_arr, right_arr)
			}
			Expr::Eq(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Neq(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = neq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Lt(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = lt(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Lte(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = lt_eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Gt(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = gt(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::Gte(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let res = gt_eq(&left_arr, &right_arr)?;
				Ok(Arc::new(res))
			}
			Expr::And(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let l_bool = left_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "AND left operand must be boolean".to_string(),
					})?;
				let r_bool = right_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "AND right operand must be boolean".to_string(),
					})?;
				let res = and_kleene(l_bool, r_bool)?;
				Ok(Arc::new(res))
			}
			Expr::Or(left, right) => {
				let left_arr = self.evaluate_expr(left, batch)?;
				let right_arr = self.evaluate_expr(right, batch)?;
				let l_bool = left_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "OR left operand must be boolean".to_string(),
					})?;
				let r_bool = right_arr
					.as_any()
					.downcast_ref::<BooleanArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: "OR right operand must be boolean".to_string(),
					})?;
				let res = or_kleene(l_bool, r_bool)?;
				Ok(Arc::new(res))
			}
			Expr::Not(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: "NOT operand must be boolean".to_string(),
					}
				})?;
				let res = not(bool_arr)?;
				Ok(Arc::new(res))
			}
			Expr::InList {
				expr,
				list,
				negated,
			} => {
				let expr_arr = self.evaluate_expr(expr, batch)?;
				let item_arrs = list
					.iter()
					.map(|item| self.evaluate_expr(item, batch))
					.collect::<Result<Vec<_>, _>>()?;
				evaluate_in_list(&expr_arr, &item_arrs, *negated, batch.num_rows())
			}
			Expr::Like {
				expr,
				pattern,
				negated,
				case_insensitive,
			} => {
				let expr_arr = self.evaluate_expr(expr, batch)?;
				evaluate_like(&expr_arr, pattern, *negated, *case_insensitive)
			}
			Expr::Case {
				when_then,
				else_expr,
			} => {
				if when_then.is_empty() {
					return Err(ExecutionError::InvalidExpression {
						message: "CASE must have at least one WHEN branch".to_string(),
					});
				}
				let when_then = when_then
					.iter()
					.map(|(condition, result)| {
						Ok((
							self.evaluate_expr(condition, batch)?,
							self.evaluate_expr(result, batch)?,
						))
					})
					.collect::<Result<Vec<_>, ExecutionError>>()?;
				let else_array = if let Some(else_expr) = else_expr {
					self.evaluate_expr(else_expr, batch)?
				} else {
					let data_type = when_then
						.first()
						.map(|(_, result)| result.data_type().clone())
						.ok_or_else(|| ExecutionError::InvalidExpression {
							message: "CASE must have at least one result branch".to_string(),
						})?;
					arrow::array::new_null_array(&data_type, batch.num_rows())
				};
				evaluate_case(&when_then, else_array)
			}
			Expr::IsNull(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let res = is_null(&arr)?;
				Ok(Arc::new(res))
			}
			Expr::IsNotNull(inner) => {
				let arr = self.evaluate_expr(inner, batch)?;
				let res = is_not_null(&arr)?;
				Ok(Arc::new(res))
			}
			Expr::ScalarFunction {
				id,
				args,
				parameter,
			} => {
				if matches!(id, ScalarFunctionId::Extract | ScalarFunctionId::DateTrunc) {
					let [inner] = args.as_slice() else {
						return Err(ExecutionError::InvalidExpression {
							message: "temporal scalar function expects one operand".to_string(),
						});
					};
					let (timestamp, zone) = match crate::ir::as_at_time_zone(inner) {
						Some((timestamp, zone)) => (timestamp, Some(zone.to_string())),
						None => (inner, None),
					};
					if *id == ScalarFunctionId::DateTrunc && zone.is_none() {
						return Err(ExecutionError::InvalidExpression {
							message: "unbound date_trunc requires explicit AT TIME ZONE"
								.to_string(),
						});
					}
					let arr = self.evaluate_expr(timestamp, batch)?;
					if *id == ScalarFunctionId::Extract
						&& zone.is_none() && arr.data_type() != &DataType::Date32
					{
						return Err(ExecutionError::InvalidExpression {
							message: "unbound EXTRACT on Timestamp requires explicit AT TIME ZONE"
								.to_string(),
						});
					}
					return match (id, parameter) {
						(
							ScalarFunctionId::Extract,
							crate::ir::ScalarFunctionParameter::Extract(payload),
						) => evaluate_extract(payload.field, zone.as_deref(), &arr),
						(
							ScalarFunctionId::DateTrunc,
							crate::ir::ScalarFunctionParameter::DateTrunc(payload),
						) => evaluate_date_trunc(
							payload.field,
							zone.as_deref()
								.expect("unbound date_trunc zone checked above"),
							&arr,
						),
						_ => Err(ExecutionError::InvalidExpression {
							message: "temporal scalar function has the wrong parameter".to_string(),
						}),
					};
				}
				let args = args
					.iter()
					.map(|arg| self.evaluate_expr(arg, batch))
					.collect::<Result<Vec<_>, ExecutionError>>()?;
				evaluate_scalar_function(*id, &args, parameter, batch.num_rows())
			}
			Expr::IsDistinctFrom {
				left,
				right,
				negated,
			} => {
				// Mirrors the binder's ADR 566 composition over the same
				// kernels; each operand is evaluated once and the arrays are
				// reused, matching the bound program's shared slots.
				let left = self.evaluate_expr(left, batch)?;
				let right = self.evaluate_expr(right, batch)?;
				let both_null = and_kleene(&is_null(&left)?, &is_null(&right)?)?;
				let both_present = and_kleene(&is_not_null(&left)?, &is_not_null(&right)?)?;
				let equal = eq(&left, &right)?;
				let not_distinct = or_kleene(&both_null, &and_kleene(&both_present, &equal)?)?;
				if *negated {
					return Ok(Arc::new(not_distinct));
				}
				Ok(Arc::new(not(&not_distinct)?))
			}
		}
	}

	pub fn evaluate_bound_program(
		&self,
		program: &BoundExprProgram,
		batch: &RecordBatch,
	) -> Result<Vec<ArrayRef>, ExecutionError> {
		Self::evaluate_bound_program_without_storage(program, batch)
	}

	pub fn evaluate_bound_program_without_storage(
		program: &BoundExprProgram,
		batch: &RecordBatch,
	) -> Result<Vec<ArrayRef>, ExecutionError> {
		program
			.validate()
			.map_err(|error| ExecutionError::InvalidExpression {
				message: format!("invalid bound expression program: {error:?}"),
			})?;
		let mut outputs: Vec<Option<ArrayRef>> = vec![None; program.nodes.len()];
		for (idx, node) in program.nodes.iter().enumerate() {
			let output = match node {
				BoundExprNode::Column(column) => {
					let ordinal = column.ordinal.0 as usize;
					batch.columns().get(ordinal).cloned().ok_or_else(|| {
						ExecutionError::InvalidExpression {
							message: format!("column ordinal {ordinal} out of bounds"),
						}
					})?
				}
				BoundExprNode::Literal(literal) => {
					let value = program.literals.get(*literal as usize).ok_or_else(|| {
						ExecutionError::InvalidExpression {
							message: format!("literal slot {literal} out of bounds"),
						}
					})?;
					literal_array(value, batch.num_rows())?
				}
				BoundExprNode::NullLiteral => {
					arrow::array::new_null_array(&DataType::Null, batch.num_rows())
				}
				BoundExprNode::Cast { input, target } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_cast(&arr, target)?
				}
				BoundExprNode::TryCast { input, target } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_try_cast(&arr, target)?
				}
				BoundExprNode::Binary { op, left, right } => {
					let left_arr = evaluated_child(&outputs, *left)?;
					let right_arr = evaluated_child(&outputs, *right)?;
					evaluate_bound_binary(program, op, *left, &left_arr, &right_arr)?
				}
				BoundExprNode::Unary { op, input } => {
					let arr = evaluated_child(&outputs, *input)?;
					evaluate_bound_unary(op, &arr)?
				}
				BoundExprNode::InList {
					expr,
					list,
					negated,
				} => {
					let expr_arr = evaluated_child(&outputs, *expr)?;
					let item_arrs = list
						.iter()
						.map(|item| evaluated_child(&outputs, *item))
						.collect::<Result<Vec<_>, _>>()?;
					evaluate_in_list(&expr_arr, &item_arrs, *negated, batch.num_rows())?
				}
				BoundExprNode::Like {
					expr,
					pattern,
					negated,
					case_insensitive,
				} => {
					let expr_arr = evaluated_child(&outputs, *expr)?;
					evaluate_like(&expr_arr, pattern, *negated, *case_insensitive)?
				}
				BoundExprNode::Case {
					when_then,
					else_input,
				} => {
					let when_then = when_then
						.iter()
						.map(|(condition, result)| {
							Ok((
								evaluated_child(&outputs, *condition)?,
								evaluated_child(&outputs, *result)?,
							))
						})
						.collect::<Result<Vec<_>, ExecutionError>>()?;
					let else_array = evaluated_child(&outputs, *else_input)?;
					evaluate_case(&when_then, else_array)?
				}
				BoundExprNode::ScalarFunction {
					id,
					args,
					parameter,
				} => {
					let args = args
						.iter()
						.map(|arg| evaluated_child(&outputs, *arg))
						.collect::<Result<Vec<_>, ExecutionError>>()?;
					evaluate_scalar_function(*id, &args, parameter, batch.num_rows())?
				}
			};
			outputs[idx] = Some(output);
		}
		program
			.roots
			.iter()
			.map(|root| evaluated_child(&outputs, *root))
			.collect()
	}

	pub(in crate::exec) fn evaluate_bound_single_root(
		&self,
		program: &BoundExprProgram,
		batch: &RecordBatch,
		context: &'static str,
	) -> Result<ArrayRef, ExecutionError> {
		let mut roots = self.evaluate_bound_program(program, batch)?.into_iter();
		let Some(root) = roots.next() else {
			return Err(ExecutionError::InvalidExpression {
				message: format!("{context} must have exactly one root"),
			});
		};
		if roots.next().is_some() {
			return Err(ExecutionError::InvalidExpression {
				message: format!("{context} must have exactly one root"),
			});
		}
		Ok(root)
	}
}

fn evaluated_child(
	outputs: &[Option<ArrayRef>],
	id: BoundExprId,
) -> Result<ArrayRef, ExecutionError> {
	outputs
		.get(id.0 as usize)
		.and_then(|array| array.as_ref())
		.cloned()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("bound expression child {:?} has not been evaluated", id),
		})
}

pub(in crate::exec) fn literal_array(
	value: &ScalarValue,
	num_rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	Ok(match value {
		ScalarValue::Boolean(value) => Arc::new(BooleanArray::from(vec![*value; num_rows])),
		ScalarValue::Int32(value) => Arc::new(Int32Array::from(vec![*value; num_rows])),
		ScalarValue::Int64(value) => Arc::new(Int64Array::from(vec![*value; num_rows])),
		ScalarValue::Date32(value) => Arc::new(Date32Array::from(vec![*value; num_rows])),
		ScalarValue::Timestamp { value, unit } => timestamp_literal_array(value, *unit, num_rows),
		ScalarValue::Interval(value) => {
			Arc::new(DurationNanosecondArray::from(vec![*value; num_rows]))
		}
		ScalarValue::CalendarMonthInterval(value) => {
			Arc::new(IntervalYearMonthArray::from(vec![*value; num_rows]))
		}
		ScalarValue::Float32(value) => Arc::new(Float32Array::from(vec![*value; num_rows])),
		ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![*value; num_rows])),
		ScalarValue::Decimal128 {
			value,
			precision,
			scale,
		} => Arc::new(
			Decimal128Array::from(vec![*value; num_rows])
				.with_precision_and_scale(*precision, *scale)
				.map_err(|error| ExecutionError::Kernel {
					message: error.to_string(),
				})?,
		),
		ScalarValue::Utf8(value) => {
			Arc::new(StringArray::from(vec![value.clone().as_deref(); num_rows]))
		}
	})
}

fn timestamp_literal_array(value: &Option<i64>, unit: TimeUnit, num_rows: usize) -> ArrayRef {
	match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(vec![*value; num_rows]).with_timezone("UTC"))
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	}
}

fn evaluate_bound_binary(
	program: &BoundExprProgram,
	op: &BoundBinaryOp,
	left: BoundExprId,
	left_arr: &ArrayRef,
	right_arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	match op {
		BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div => {
			let arithmetic_op = match op {
				BoundBinaryOp::Add => ArithmeticOp::Add,
				BoundBinaryOp::Sub => ArithmeticOp::Sub,
				BoundBinaryOp::Mul => ArithmeticOp::Mul,
				BoundBinaryOp::Div => ArithmeticOp::Div,
				_ => unreachable!("arithmetic op matched"),
			};
			if let Some(result) = evaluate_temporal_arithmetic(arithmetic_op, left_arr, right_arr)?
			{
				return Ok(result);
			}
			// ADR 263: Decimal128 * Decimal128 — Arrow's numeric `mul` widens to
			// `Decimal128(p1+p2+1, s1+s2)`, matching the binder's bound output type (the operands keep
			// their own (p,s), so this bypasses `evaluate_arithmetic`'s operands-equal-output path).
			if matches!(arithmetic_op, ArithmeticOp::Mul)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return Ok(arrow::compute::kernels::numeric::mul(left_arr, right_arr)?);
			}
			// ADR 266: Decimal128 +/- Decimal128 — Arrow's numeric `add`/`sub` carry the scale and add one
			// precision digit, matching the binder's same-(p,s) bound output type (operands keep their
			// (p,s), bypassing `evaluate_arithmetic`'s operands-equal-output path).
			if matches!(arithmetic_op, ArithmeticOp::Add | ArithmeticOp::Sub)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return Ok(match arithmetic_op {
					ArithmeticOp::Add => {
						arrow::compute::kernels::numeric::add(left_arr, right_arr)?
					}
					ArithmeticOp::Sub => {
						arrow::compute::kernels::numeric::sub(left_arr, right_arr)?
					}
					_ => unreachable!("add/sub matched"),
				});
			}
			// ADR 271: Decimal128 / Decimal128 — a custom Toron i128 divide (NOT Arrow `numeric::div`, which
			// errors on a zero divisor). div-by-zero -> NULL (ADR 084), half-away-from-zero rounding (ADR
			// 268), stamped with the shared validated output (p,s).
			if matches!(arithmetic_op, ArithmeticOp::Div)
				&& matches!(left_arr.data_type(), DataType::Decimal128(_, _))
				&& matches!(right_arr.data_type(), DataType::Decimal128(_, _))
			{
				return evaluate_decimal_division(left_arr, right_arr);
			}
			let left_type =
				program
					.node_type(left)
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: format!("missing type for bound expression {:?}", left),
					})?;
			evaluate_arithmetic(arithmetic_op, left_arr, right_arr, left_type)
		}
		BoundBinaryOp::Eq => Ok(Arc::new(eq(left_arr, right_arr)?)),
		BoundBinaryOp::Neq => Ok(Arc::new(neq(left_arr, right_arr)?)),
		BoundBinaryOp::Lt => Ok(Arc::new(lt(left_arr, right_arr)?)),
		BoundBinaryOp::Lte => Ok(Arc::new(lt_eq(left_arr, right_arr)?)),
		BoundBinaryOp::Gt => Ok(Arc::new(gt(left_arr, right_arr)?)),
		BoundBinaryOp::Gte => Ok(Arc::new(gt_eq(left_arr, right_arr)?)),
		BoundBinaryOp::And => {
			let left_bool = as_boolean(left_arr, "AND left operand")?;
			let right_bool = as_boolean(right_arr, "AND right operand")?;
			Ok(Arc::new(and_kleene(left_bool, right_bool)?))
		}
		BoundBinaryOp::Or => {
			let left_bool = as_boolean(left_arr, "OR left operand")?;
			let right_bool = as_boolean(right_arr, "OR right operand")?;
			Ok(Arc::new(or_kleene(left_bool, right_bool)?))
		}
	}
}

fn evaluate_bound_unary(op: &BoundUnaryOp, arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	match op {
		BoundUnaryOp::Not => {
			let bool_arr = as_boolean(arr, "NOT operand")?;
			Ok(Arc::new(not(bool_arr)?))
		}
		BoundUnaryOp::IsNull => Ok(Arc::new(is_null(arr)?)),
		BoundUnaryOp::IsNotNull => Ok(Arc::new(is_not_null(arr)?)),
	}
}

fn evaluate_scalar_function(
	id: ScalarFunctionId,
	args: &[ArrayRef],
	parameter: &crate::ir::ScalarFunctionParameter,
	num_rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	let metadata = crate::ir::scalar_function_metadata(id);
	if !metadata.arity.accepts(args.len()) {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"internal: {} received {} arguments after binding",
				metadata.sql_name,
				args.len()
			),
		});
	}
	match (id, parameter) {
		(ScalarFunctionId::Extract, crate::ir::ScalarFunctionParameter::Extract(payload)) => {
			return evaluate_extract(payload.field, payload.zone.as_deref(), &args[0]);
		}
		(ScalarFunctionId::DateTrunc, crate::ir::ScalarFunctionParameter::DateTrunc(payload)) => {
			return payload
				.zone
				.as_deref()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "bound date_trunc is missing its timezone".to_string(),
				})
				.and_then(|zone| evaluate_date_trunc(payload.field, zone, &args[0]));
		}
		(
			ScalarFunctionId::Trim | ScalarFunctionId::Ltrim | ScalarFunctionId::Rtrim,
			crate::ir::ScalarFunctionParameter::Trim(payload),
		) => {
			return evaluate_trim(args, payload.where_field);
		}
		(
			ScalarFunctionId::ToTimestamp | ScalarFunctionId::TryToTimestamp,
			crate::ir::ScalarFunctionParameter::ToTimestamp { zone },
		) => {
			return evaluate_to_timestamp(&args[0], zone, metadata.failure);
		}
		(
			ScalarFunctionId::ToTimestamp | ScalarFunctionId::TryToTimestamp,
			crate::ir::ScalarFunctionParameter::TemporalFormat { pattern, zone },
		) => {
			let zone = zone
				.as_deref()
				.ok_or_else(|| ExecutionError::InvalidExpression {
					message: "bound TO_TIMESTAMP is missing its timezone".to_string(),
				})?;
			return evaluate_to_timestamp_with_pattern(&args[0], pattern, zone, metadata.failure);
		}
		(
			ScalarFunctionId::ToChar,
			crate::ir::ScalarFunctionParameter::TemporalFormat { pattern, zone },
		) => {
			return evaluate_to_char(&args[0], pattern, zone.as_deref());
		}
		(
			ScalarFunctionId::ToDate | ScalarFunctionId::TryToDate,
			crate::ir::ScalarFunctionParameter::None,
		) => {
			return evaluate_to_date(&args[0], metadata.failure);
		}
		(ScalarFunctionId::Ltrim, crate::ir::ScalarFunctionParameter::None) => {
			return evaluate_trim(args, crate::ir::TrimWhere::Leading);
		}
		(ScalarFunctionId::Rtrim, crate::ir::ScalarFunctionParameter::None) => {
			return evaluate_trim(args, crate::ir::TrimWhere::Trailing);
		}
		(
			ScalarFunctionId::Now,
			crate::ir::ScalarFunctionParameter::CurrentTimestamp { timestamp_nanos },
		) => {
			return Ok(timestamp_literal_array(
				&Some(*timestamp_nanos),
				TimeUnit::Nanosecond,
				num_rows,
			));
		}
		(
			ScalarFunctionId::CurrentDate,
			crate::ir::ScalarFunctionParameter::CurrentDate { days },
		) => {
			return Ok(Arc::new(Date32Array::from(vec![Some(*days); num_rows])) as ArrayRef);
		}
		_ => {}
	}
	if !matches!(parameter, crate::ir::ScalarFunctionParameter::None) {
		return Err(ExecutionError::InvalidExpression {
			message: "scalar function has the wrong parameter".to_string(),
		});
	}
	match (
		metadata.evaluator,
		metadata.null_behavior,
		metadata.non_finite_behavior,
	) {
		(
			ScalarFunctionEvaluator::Exp,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_exp(&args[0]),
		(
			ScalarFunctionEvaluator::Sqrt,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_float(&args[0], "SQRT", f32::sqrt, f64::sqrt),
		(
			ScalarFunctionEvaluator::Pow,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_pow(&args[0], &args[1]),
		(
			ScalarFunctionEvaluator::Abs,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_abs(&args[0]),
		(
			ScalarFunctionEvaluator::Ln,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_float(&args[0], "LN", f32::ln, f64::ln),
		(
			ScalarFunctionEvaluator::Log,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_log(&args[0], &args[1]),
		(
			ScalarFunctionEvaluator::Round,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_round(&args[0], args.get(1)),
		(
			ScalarFunctionEvaluator::Ceil,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_float(&args[0], "CEIL", f32::ceil, f64::ceil),
		(
			ScalarFunctionEvaluator::Floor,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Rejects,
		) => evaluate_floor(&args[0]),
		(
			ScalarFunctionEvaluator::Trunc,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_trunc(&args[0]),
		(
			ScalarFunctionEvaluator::Sign,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_sign(&args[0]),
		(
			ScalarFunctionEvaluator::Mod,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_mod(&args[0], &args[1]),
		(
			ScalarFunctionEvaluator::Upper,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_string(&args[0], "UPPER", str::to_uppercase),
		(
			ScalarFunctionEvaluator::Lower,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_string(&args[0], "LOWER", str::to_lowercase),
		(
			ScalarFunctionEvaluator::Initcap,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_string(&args[0], "INITCAP", initcap),
		(
			ScalarFunctionEvaluator::Reverse,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_string(&args[0], "REVERSE", reverse),
		(
			ScalarFunctionEvaluator::Trim,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_unary_string(&args[0], "TRIM", |value| value.trim().to_string()),
		(
			ScalarFunctionEvaluator::Concat,
			crate::ir::ScalarFunctionNullBehavior::Ignores,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_concat(args),
		(
			ScalarFunctionEvaluator::StringConcat,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_string_concat(args),
		(
			ScalarFunctionEvaluator::Position,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_position(&args[0], &args[1]),
		(
			ScalarFunctionEvaluator::Replace,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_replace(&args[0], &args[1], &args[2]),
		(
			ScalarFunctionEvaluator::Lpad,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_pad(args, true),
		(
			ScalarFunctionEvaluator::Rpad,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_pad(args, false),
		(
			ScalarFunctionEvaluator::SplitPart,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_split_part(&args[0], &args[1], &args[2]),
		(
			ScalarFunctionEvaluator::CharLength,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_char_length(&args[0]),
		(
			ScalarFunctionEvaluator::AtTimeZone,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => {
			// The zone reaches the executor as a constant string argument; the
			// binder already proved it a literal and a valid IANA name.
			let zone = constant_string_argument(&args[1], "AT TIME ZONE")?;
			evaluate_at_time_zone(&args[0], &zone)
		}
		(
			ScalarFunctionEvaluator::TimeBucket,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => {
			if args[1].is_empty() {
				return evaluate_time_bucket(std::time::Duration::from_millis(1), &args[1]);
			}
			// The binder proved the interval is a positive interval literal, so
			// the constant array repeats one value. `evaluate_time_bucket` stays
			// the single implementation of the bucket arithmetic.
			let nanos = constant_interval_nanos(&args[0])?;
			evaluate_time_bucket(std::time::Duration::from_nanos(nanos), &args[1])
		}
		(
			ScalarFunctionEvaluator::Substring,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => {
			// The binder proved both bounds are integer literals, so the constant
			// array carries one repeated value and row 0 is the parameter. An
			// empty batch has no row 0 and no output either, so it never reads
			// them.
			if args[0].is_empty() {
				return evaluate_substring(&args[0], 1, None);
			}
			let from = constant_integer_argument(&args[1], "SUBSTRING FROM")?;
			let length = match args.get(2) {
				Some(length) => Some(
					u64::try_from(constant_integer_argument(length, "SUBSTRING FOR")?).map_err(
						|_| ExecutionError::InvalidExpression {
							message: "SUBSTRING FOR must be >= 0".to_string(),
						},
					)?,
				),
				None => None,
			};
			evaluate_substring(&args[0], from, length)
		}
		(
			ScalarFunctionEvaluator::WidthBucket,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_width_bucket(&args[0], &args[1], &args[2], &args[3]),
		(
			ScalarFunctionEvaluator::Greatest,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_ordered_extreme(args, OrderedExtreme::Greatest, "GREATEST"),
		(
			ScalarFunctionEvaluator::Least,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => evaluate_ordered_extreme(args, OrderedExtreme::Least, "LEAST"),
		(
			ScalarFunctionEvaluator::Random,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => Ok(Arc::new(
			(0..num_rows)
				.map(|_| Some(rand::random::<f64>()))
				.collect::<Float64Array>(),
		) as ArrayRef),
		(
			ScalarFunctionEvaluator::Extract | ScalarFunctionEvaluator::DateTrunc,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"internal: {} has an invalid temporal parameter",
				metadata.sql_name
			),
		}),
		(
			ScalarFunctionEvaluator::Floor,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Propagates,
		)
		| (
			ScalarFunctionEvaluator::Abs
			| ScalarFunctionEvaluator::AtTimeZone
			| ScalarFunctionEvaluator::Ceil
			| ScalarFunctionEvaluator::CharLength
			| ScalarFunctionEvaluator::Concat
			| ScalarFunctionEvaluator::CurrentDate
			| ScalarFunctionEvaluator::DateTrunc
			| ScalarFunctionEvaluator::Exp
			| ScalarFunctionEvaluator::Extract
			| ScalarFunctionEvaluator::Initcap
			| ScalarFunctionEvaluator::Ln
			| ScalarFunctionEvaluator::Log
			| ScalarFunctionEvaluator::Lpad
			| ScalarFunctionEvaluator::Lower
			| ScalarFunctionEvaluator::Mod
			| ScalarFunctionEvaluator::Now
			| ScalarFunctionEvaluator::Position
			| ScalarFunctionEvaluator::Pow
			| ScalarFunctionEvaluator::Rpad
			| ScalarFunctionEvaluator::Replace
			| ScalarFunctionEvaluator::Round
			| ScalarFunctionEvaluator::Sign
			| ScalarFunctionEvaluator::Sqrt
			| ScalarFunctionEvaluator::SplitPart
			| ScalarFunctionEvaluator::StringConcat
			| ScalarFunctionEvaluator::Substring
			| ScalarFunctionEvaluator::TimeBucket
			| ScalarFunctionEvaluator::Trim
			| ScalarFunctionEvaluator::Upper
			| ScalarFunctionEvaluator::Greatest
			| ScalarFunctionEvaluator::Least
			| ScalarFunctionEvaluator::Random
			| ScalarFunctionEvaluator::WidthBucket,
			crate::ir::ScalarFunctionNullBehavior::Propagates,
			crate::ir::ScalarFunctionNonFiniteBehavior::Rejects,
		) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"internal: {} has an unsupported scalar-function behavior tuple",
				metadata.sql_name
			),
		}),
		_ => Err(ExecutionError::InvalidExpression {
			message: format!(
				"internal: {} has an unsupported scalar-function behavior tuple",
				metadata.sql_name
			),
		}),
	}
}

/// Which end of the ordering `GREATEST` and `LEAST` keep.
#[derive(Clone, Copy)]
enum OrderedExtreme {
	Greatest,
	Least,
}

impl OrderedExtreme {
	fn keeps<T: PartialOrd>(self, candidate: &T, incumbent: &T) -> bool {
		match self {
			Self::Greatest => candidate > incumbent,
			Self::Least => candidate < incumbent,
		}
	}
}

/// `GREATEST` and `LEAST` are not ANSI, and the dialects disagree about NULL.
/// This engine propagates — the registry says so, and this reads it rather than
/// deciding again here, so the two cannot drift apart.
fn evaluate_ordered_extreme(
	args: &[ArrayRef],
	extreme: OrderedExtreme,
	name: &'static str,
) -> Result<ArrayRef, ExecutionError> {
	macro_rules! fold_primitive {
		($array:ty, $builder:expr) => {
			if args[0].as_any().downcast_ref::<$array>().is_some() {
				let columns = args
					.iter()
					.map(|arg| {
						arg.as_any().downcast_ref::<$array>().ok_or_else(|| {
							ExecutionError::InvalidExpression {
								message: format!(
									"{name} expects every argument to share one type, got {:?}",
									arg.data_type()
								),
							}
						})
					})
					.collect::<Result<Vec<_>, _>>()?;
				let rows = columns[0].len();
				let mut values = Vec::with_capacity(rows);
				for row in 0..rows {
					let mut best = None;
					for column in &columns {
						if !column.is_valid(row) {
							best = None;
							break;
						}
						let candidate = column.value(row);
						best = match best {
							Some(incumbent) if !extreme.keeps(&candidate, &incumbent) => {
								Some(incumbent)
							}
							_ => Some(candidate),
						};
					}
					values.push(best);
				}
				return Ok($builder(values));
			}
		};
	}

	fold_primitive!(Int32Array, |values| Arc::new(Int32Array::from_iter(values))
		as ArrayRef);
	fold_primitive!(Int64Array, |values| Arc::new(Int64Array::from_iter(values))
		as ArrayRef);
	fold_primitive!(
		Float32Array,
		|values| Arc::new(Float32Array::from_iter(values)) as ArrayRef
	);
	fold_primitive!(
		Float64Array,
		|values| Arc::new(Float64Array::from_iter(values)) as ArrayRef
	);
	fold_primitive!(StringArray, |values: Vec<Option<&str>>| Arc::new(
		values.into_iter().collect::<StringArray>()
	) as ArrayRef);
	fold_primitive!(LargeStringArray, |values: Vec<Option<&str>>| Arc::new(
		values.into_iter().collect::<LargeStringArray>()
	) as ArrayRef);

	if let Some(first) = args[0].as_any().downcast_ref::<Decimal128Array>() {
		let (precision, scale) = match first.data_type() {
			DataType::Decimal128(precision, scale) => (*precision, *scale),
			other => {
				return Err(ExecutionError::InvalidExpression {
					message: format!("{name} expects an ordered type, got {other:?}"),
				});
			}
		};
		let columns = args
			.iter()
			.map(|arg| {
				arg.as_any()
					.downcast_ref::<Decimal128Array>()
					.filter(|column| column.data_type() == first.data_type())
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: format!(
							"{name} expects every argument to share one type, got {:?}",
							arg.data_type()
						),
					})
			})
			.collect::<Result<Vec<_>, _>>()?;
		let rows = columns[0].len();
		let mut values = Vec::with_capacity(rows);
		for row in 0..rows {
			let mut best = None;
			for column in &columns {
				if !column.is_valid(row) {
					best = None;
					break;
				}
				let candidate = column.value(row);
				best = match best {
					Some(incumbent) if !extreme.keeps(&candidate, &incumbent) => Some(incumbent),
					_ => Some(candidate),
				};
			}
			values.push(best);
		}
		return Ok(Arc::new(
			Decimal128Array::from_iter(values).with_precision_and_scale(precision, scale)?,
		) as ArrayRef);
	}

	Err(ExecutionError::InvalidExpression {
		message: format!(
			"{name} expects an ordered numeric or string type, got {:?}",
			args[0].data_type()
		),
	})
}

/// `WIDTH_BUCKET(operand, low, high, count)` answers the ANSI question: which of
/// `count` equal-width buckets spanning `[low, high)` contains `operand`. The
/// indexing convention is 1-based, `0` is the underflow bucket, and `count + 1`
/// is the overflow bucket — those are the standard's answers, not choices made
/// here.
///
/// Every operand is read per row, so an invalid range is a row-level failure
/// rather than something the binder could have caught.
fn evaluate_width_bucket(
	operand: &ArrayRef,
	low: &ArrayRef,
	high: &ArrayRef,
	count: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let operands = float_operands(operand, "operand")?;
	let lows = float_operands(low, "low")?;
	let highs = float_operands(high, "high")?;
	let counts = integer_bucket_counts(count)?;

	let mut buckets = Vec::with_capacity(operands.len());
	for row in 0..operands.len() {
		let (Some(operand), Some(low), Some(high), Some(count)) =
			(operands[row], lows[row], highs[row], counts[row])
		else {
			buckets.push(None);
			continue;
		};
		if count <= 0 {
			return Err(ExecutionError::InvalidExpression {
				message: format!("WIDTH_BUCKET expects a positive bucket count, got {count}"),
			});
		}
		// Reversed ranges are a Postgres extension, not ANSI. Refusing keeps the
		// direction convention from being decided by accident here.
		if !(low < high) {
			return Err(ExecutionError::InvalidExpression {
				message: format!("WIDTH_BUCKET expects low < high, got low {low} and high {high}"),
			});
		}
		buckets.push(Some(if operand < low {
			0
		} else if operand >= high {
			count.saturating_add(1)
		} else {
			let width = (high - low) / count as f64;
			let offset = ((operand - low) / width).floor();
			// Clamp guards the boundary case where floating division lands one
			// past the last bucket for an operand that is strictly below `high`.
			1 + (offset as i64).clamp(0, count - 1)
		}));
	}
	Ok(Arc::new(Int64Array::from_iter(buckets)) as ArrayRef)
}

fn float_operands(input: &ArrayRef, role: &str) -> Result<Vec<Option<f64>>, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Float32Array>() {
		return Ok(input.iter().map(|value| value.map(f64::from)).collect());
	}
	if let Some(input) = input.as_any().downcast_ref::<Float64Array>() {
		return Ok(input.iter().collect());
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"WIDTH_BUCKET expects a float {role}, got {:?}",
			input.data_type()
		),
	})
}

fn integer_bucket_counts(input: &ArrayRef) -> Result<Vec<Option<i64>>, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Int32Array>() {
		return Ok(input.iter().map(|value| value.map(i64::from)).collect());
	}
	if let Some(input) = input.as_any().downcast_ref::<Int64Array>() {
		return Ok(input.iter().collect());
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"WIDTH_BUCKET expects an integer bucket count, got {:?}",
			input.data_type()
		),
	})
}

/// The output keeps the input's offset width, so a `LargeUtf8` column stays
/// `LargeUtf8` rather than being silently narrowed by passing through a function.
fn evaluate_unary_string(
	input: &ArrayRef,
	name: &'static str,
	transform: impl Fn(&str) -> String,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<StringArray>() {
		return Ok(Arc::new(
			input
				.iter()
				.map(|value| value.map(&transform))
				.collect::<StringArray>(),
		) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<LargeStringArray>() {
		return Ok(Arc::new(
			input
				.iter()
				.map(|value| value.map(&transform))
				.collect::<LargeStringArray>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("{name} expects a string input, got {:?}", input.data_type()),
	})
}

fn reverse(value: &str) -> String {
	value.chars().rev().collect()
}

fn initcap(value: &str) -> String {
	let mut capitalize = true;
	let mut output = String::with_capacity(value.len());
	for character in value.chars() {
		if character.is_alphanumeric() {
			if capitalize {
				output.extend(character.to_uppercase());
			} else {
				output.extend(character.to_lowercase());
			}
			capitalize = false;
		} else {
			output.push(character);
			capitalize = true;
		}
	}
	output
}

fn evaluate_trim(
	args: &[ArrayRef],
	where_field: crate::ir::TrimWhere,
) -> Result<ArrayRef, ExecutionError> {
	if let [input] = args {
		return evaluate_unary_string(input, "TRIM", |value| trim_string(value, None, where_field));
	}
	let [input, characters] = args else {
		return Err(ExecutionError::InvalidExpression {
			message: "TRIM expects one or two string arguments".to_string(),
		});
	};
	if let (Some(input), Some(characters)) = (
		input.as_any().downcast_ref::<StringArray>(),
		characters.as_any().downcast_ref::<StringArray>(),
	) {
		return Ok(Arc::new(
			input
				.iter()
				.zip(characters.iter())
				.map(|(value, chars)| match (value, chars) {
					(Some(value), Some(chars)) => {
						Some(trim_string(value, Some(chars), where_field))
					}
					_ => None,
				})
				.collect::<StringArray>(),
		) as ArrayRef);
	}
	if let (Some(input), Some(characters)) = (
		input.as_any().downcast_ref::<LargeStringArray>(),
		characters.as_any().downcast_ref::<LargeStringArray>(),
	) {
		return Ok(Arc::new(
			input
				.iter()
				.zip(characters.iter())
				.map(|(value, chars)| match (value, chars) {
					(Some(value), Some(chars)) => {
						Some(trim_string(value, Some(chars), where_field))
					}
					_ => None,
				})
				.collect::<LargeStringArray>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"TRIM expects its character set to match the input string type, got {:?} and {:?}",
			input.data_type(),
			characters.data_type()
		),
	})
}

fn trim_string(value: &str, characters: Option<&str>, where_field: crate::ir::TrimWhere) -> String {
	match characters {
		None => match where_field {
			crate::ir::TrimWhere::Both => value.trim().to_string(),
			crate::ir::TrimWhere::Leading => value.trim_start().to_string(),
			crate::ir::TrimWhere::Trailing => value.trim_end().to_string(),
		},
		Some(characters) => match where_field {
			crate::ir::TrimWhere::Both => value
				.trim_matches(|character| characters.contains(character))
				.to_string(),
			crate::ir::TrimWhere::Leading => value
				.trim_start_matches(|character| characters.contains(character))
				.to_string(),
			crate::ir::TrimWhere::Trailing => value
				.trim_end_matches(|character| characters.contains(character))
				.to_string(),
		},
	}
}

fn evaluate_concat(args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
	evaluate_concat_with_semantics(args, ConcatSemantics::NullIgnoring)
}

fn evaluate_string_concat(args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
	evaluate_concat_with_semantics(args, ConcatSemantics::NullPropagating)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConcatSemantics {
	NullIgnoring,
	NullPropagating,
}

impl ConcatSemantics {
	const fn name(self) -> &'static str {
		match self {
			Self::NullIgnoring => "CONCAT",
			Self::NullPropagating => "||",
		}
	}

	const fn propagates_nulls(self) -> bool {
		matches!(self, Self::NullPropagating)
	}
}

fn evaluate_concat_with_semantics(
	args: &[ArrayRef],
	semantics: ConcatSemantics,
) -> Result<ArrayRef, ExecutionError> {
	let name = semantics.name();
	if let Some(first) = args[0].as_any().downcast_ref::<StringArray>() {
		let columns = args
			.iter()
			.map(|arg| {
				arg.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: format!(
							"{name} expects every argument to share one string type, got {:?}",
							arg.data_type()
						),
					}
				})
			})
			.collect::<Result<Vec<_>, _>>()?;
		let values = (0..first.len())
			.map(|row| {
				if semantics.propagates_nulls() && columns.iter().any(|column| column.is_null(row))
				{
					return None;
				}
				let mut value = String::new();
				for column in &columns {
					if column.is_valid(row) {
						value.push_str(column.value(row));
					}
				}
				Some(value)
			})
			.collect::<StringArray>();
		return Ok(Arc::new(values) as ArrayRef);
	}
	if let Some(first) = args[0].as_any().downcast_ref::<LargeStringArray>() {
		let columns = args
			.iter()
			.map(|arg| {
				arg.as_any()
					.downcast_ref::<LargeStringArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: format!(
							"{name} expects every argument to share one string type, got {:?}",
							arg.data_type()
						),
					})
			})
			.collect::<Result<Vec<_>, _>>()?;
		let values = (0..first.len())
			.map(|row| {
				if semantics.propagates_nulls() && columns.iter().any(|column| column.is_null(row))
				{
					return None;
				}
				let mut value = String::new();
				for column in &columns {
					if column.is_valid(row) {
						value.push_str(column.value(row));
					}
				}
				Some(value)
			})
			.collect::<LargeStringArray>();
		return Ok(Arc::new(values) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"{name} expects string arguments, got {:?}",
			args[0].data_type()
		),
	})
}

fn integer_value_at(
	input: &ArrayRef,
	row: usize,
	name: &'static str,
) -> Result<Option<i64>, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Int32Array>() {
		return Ok(Some(i64::from(input.value(row))));
	}
	if let Some(input) = input.as_any().downcast_ref::<Int64Array>() {
		return Ok(Some(input.value(row)));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"{name} expects an integer argument, got {:?}",
			input.data_type()
		),
	})
}

fn pad_string(
	value: &str,
	target_len: i64,
	fill: &str,
	left: bool,
) -> Result<String, ExecutionError> {
	let target_len =
		usize::try_from(target_len).map_err(|_| ExecutionError::InvalidExpression {
			message: "LPAD/RPAD target length must be non-negative".to_string(),
		})?;
	let value_chars = value.chars().collect::<Vec<_>>();
	if target_len <= value_chars.len() {
		return Ok(value_chars.into_iter().take(target_len).collect());
	}
	let fill_chars = fill.chars().collect::<Vec<_>>();
	if fill_chars.is_empty() {
		return Err(ExecutionError::InvalidExpression {
			message: "LPAD/RPAD fill string must not be empty when padding".to_string(),
		});
	}
	let mut padding = Vec::with_capacity(target_len - value_chars.len());
	while padding.len() < target_len - value_chars.len() {
		padding.extend(
			fill_chars
				.iter()
				.copied()
				.take(target_len - value_chars.len() - padding.len()),
		);
	}
	if left {
		padding.extend(value_chars);
		Ok(padding.into_iter().collect())
	} else {
		let mut output = value_chars;
		output.extend(padding);
		Ok(output.into_iter().collect())
	}
}

fn evaluate_pad(args: &[ArrayRef], left: bool) -> Result<ArrayRef, ExecutionError> {
	let ([input, length] | [input, length, _]) = args else {
		return Err(ExecutionError::InvalidExpression {
			message: "LPAD/RPAD expects two or three arguments".to_string(),
		});
	};
	let fill = args.get(2);
	if let Some(input) = input.as_any().downcast_ref::<StringArray>() {
		let fill = fill
			.map(|fill| {
				fill.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
					ExecutionError::InvalidExpression {
						message: format!(
							"LPAD/RPAD expects the fill to match the input string type, got {:?}",
							fill.data_type()
						),
					}
				})
			})
			.transpose()?;
		return Ok(Arc::new(
			(0..input.len())
				.map(|row| {
					let value = input.value(row);
					if input.is_null(row)
						|| length.is_null(row)
						|| fill.is_some_and(|fill| fill.is_null(row))
					{
						return Ok(None);
					}
					let fill = fill.map_or(" ", |fill| fill.value(row));
					Ok(Some(pad_string(
						value,
						integer_value_at(length, row, "LPAD/RPAD")?.unwrap(),
						fill,
						left,
					)?))
				})
				.collect::<Result<StringArray, ExecutionError>>()?,
		) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<LargeStringArray>() {
		let fill = fill
			.map(|fill| {
				fill.as_any()
					.downcast_ref::<LargeStringArray>()
					.ok_or_else(|| ExecutionError::InvalidExpression {
						message: format!(
							"LPAD/RPAD expects the fill to match the input string type, got {:?}",
							fill.data_type()
						),
					})
			})
			.transpose()?;
		return Ok(Arc::new(
			(0..input.len())
				.map(|row| {
					let value = input.value(row);
					if input.is_null(row)
						|| length.is_null(row)
						|| fill.is_some_and(|fill| fill.is_null(row))
					{
						return Ok(None);
					}
					let fill = fill.map_or(" ", |fill| fill.value(row));
					Ok(Some(pad_string(
						value,
						integer_value_at(length, row, "LPAD/RPAD")?.unwrap(),
						fill,
						left,
					)?))
				})
				.collect::<Result<LargeStringArray, ExecutionError>>()?,
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"LPAD/RPAD expects a string input, got {:?}",
			input.data_type()
		),
	})
}

fn split_part_string(value: &str, delimiter: &str, part: i64) -> Result<String, ExecutionError> {
	let part = usize::try_from(part).map_err(|_| ExecutionError::InvalidExpression {
		message: "SPLIT_PART part number must be positive".to_string(),
	})?;
	if part == 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "SPLIT_PART part number must be positive".to_string(),
		});
	}
	if delimiter.is_empty() {
		return Ok((part == 1).then(|| value.to_string()).unwrap_or_default());
	}
	Ok(value
		.split(delimiter)
		.nth(part - 1)
		.unwrap_or_default()
		.to_string())
}

fn evaluate_split_part(
	input: &ArrayRef,
	delimiter: &ArrayRef,
	part: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let (Some(input), Some(delimiter)) = (
		input.as_any().downcast_ref::<StringArray>(),
		delimiter.as_any().downcast_ref::<StringArray>(),
	) {
		return Ok(Arc::new(
			(0..input.len())
				.map(|row| {
					if input.is_null(row) || delimiter.is_null(row) || part.is_null(row) {
						return Ok(None);
					}
					Ok(Some(split_part_string(
						input.value(row),
						delimiter.value(row),
						integer_value_at(part, row, "SPLIT_PART")?.unwrap(),
					)?))
				})
				.collect::<Result<StringArray, ExecutionError>>()?,
		) as ArrayRef);
	}
	if let (Some(input), Some(delimiter)) = (
		input.as_any().downcast_ref::<LargeStringArray>(),
		delimiter.as_any().downcast_ref::<LargeStringArray>(),
	) {
		return Ok(Arc::new(
			(0..input.len())
				.map(|row| {
					if input.is_null(row) || delimiter.is_null(row) || part.is_null(row) {
						return Ok(None);
					}
					Ok(Some(split_part_string(
						input.value(row),
						delimiter.value(row),
						integer_value_at(part, row, "SPLIT_PART")?.unwrap(),
					)?))
				})
				.collect::<Result<LargeStringArray, ExecutionError>>()?,
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"SPLIT_PART expects two string arguments of the same type, got {:?} and {:?}",
			input.data_type(),
			delimiter.data_type()
		),
	})
}

fn string_position(needle: &str, input: &str) -> i64 {
	input
		.find(needle)
		.map(|byte| i64::try_from(input[..byte].chars().count() + 1).unwrap_or(i64::MAX))
		.unwrap_or(0)
}

fn evaluate_position(needle: &ArrayRef, input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	if let (Some(needle), Some(input)) = (
		needle.as_any().downcast_ref::<StringArray>(),
		input.as_any().downcast_ref::<StringArray>(),
	) {
		return Ok(Arc::new(
			needle
				.iter()
				.zip(input.iter())
				.map(|(needle, input)| match (needle, input) {
					(Some(needle), Some(input)) => Some(string_position(needle, input)),
					_ => None,
				})
				.collect::<Int64Array>(),
		) as ArrayRef);
	}
	if let (Some(needle), Some(input)) = (
		needle.as_any().downcast_ref::<LargeStringArray>(),
		input.as_any().downcast_ref::<LargeStringArray>(),
	) {
		return Ok(Arc::new(
			needle
				.iter()
				.zip(input.iter())
				.map(|(needle, input)| match (needle, input) {
					(Some(needle), Some(input)) => Some(string_position(needle, input)),
					_ => None,
				})
				.collect::<Int64Array>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"POSITION expects two string arguments of the same type, got {:?} and {:?}",
			needle.data_type(),
			input.data_type()
		),
	})
}

fn evaluate_replace(
	input: &ArrayRef,
	from: &ArrayRef,
	to: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let (Some(input), Some(from), Some(to)) = (
		input.as_any().downcast_ref::<StringArray>(),
		from.as_any().downcast_ref::<StringArray>(),
		to.as_any().downcast_ref::<StringArray>(),
	) {
		return Ok(Arc::new(
			input
				.iter()
				.zip(from.iter())
				.zip(to.iter())
				.map(|((input, from), to)| match (input, from, to) {
					(Some(input), Some(from), Some(to)) => Some(input.replace(from, to)),
					_ => None,
				})
				.collect::<StringArray>(),
		) as ArrayRef);
	}
	if let (Some(input), Some(from), Some(to)) = (
		input.as_any().downcast_ref::<LargeStringArray>(),
		from.as_any().downcast_ref::<LargeStringArray>(),
		to.as_any().downcast_ref::<LargeStringArray>(),
	) {
		return Ok(Arc::new(
			input
				.iter()
				.zip(from.iter())
				.zip(to.iter())
				.map(|((input, from), to)| match (input, from, to) {
					(Some(input), Some(from), Some(to)) => Some(input.replace(from, to)),
					_ => None,
				})
				.collect::<LargeStringArray>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"REPLACE expects three string arguments of the same type, got {:?}, {:?}, and {:?}",
			input.data_type(),
			from.data_type(),
			to.data_type()
		),
	})
}

/// `CHAR_LENGTH` counts characters, not bytes — `OCTET_LENGTH` is the byte
/// measure and is not admitted here. `str::len` would answer in bytes and be
/// wrong for every non-ASCII value.
fn evaluate_char_length(input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let count = |value: &str| i64::try_from(value.chars().count()).unwrap_or(i64::MAX);
	if let Some(input) = input.as_any().downcast_ref::<StringArray>() {
		return Ok(Arc::new(
			input
				.iter()
				.map(|value| value.map(count))
				.collect::<Int64Array>(),
		) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<LargeStringArray>() {
		return Ok(Arc::new(
			input
				.iter()
				.map(|value| value.map(count))
				.collect::<Int64Array>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"CHAR_LENGTH expects a string input, got {:?}",
			input.data_type()
		),
	})
}

/// `SIGN` preserves its input type, so a decimal's sign is `1` at that decimal's
/// own scale rather than an integer. `f64::signum` returns ±1 for zero, which is
/// not what SQL means by sign, so zero is handled before it.
fn evaluate_sign(input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Int32Array>() {
		return Ok(Arc::new(Int32Array::from_iter(
			input.iter().map(|value| value.map(i32::signum)),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Int64Array>() {
		return Ok(Arc::new(Int64Array::from_iter(
			input.iter().map(|value| value.map(i64::signum)),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float32Array>() {
		return Ok(Arc::new(Float32Array::from_iter(input.iter().map(|value| {
			value.map(|value| if value == 0.0 { 0.0 } else { value.signum() })
		}))) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float64Array>() {
		return Ok(Arc::new(Float64Array::from_iter(input.iter().map(|value| {
			value.map(|value| if value == 0.0 { 0.0 } else { value.signum() })
		}))) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Decimal128Array>() {
		let (precision, scale) = match input.data_type() {
			DataType::Decimal128(precision, scale) => (*precision, *scale),
			other => {
				return Err(ExecutionError::InvalidExpression {
					message: format!("SIGN expects a numeric input, got {other:?}"),
				});
			}
		};
		// One unit at this scale, so the result is 1, 0, or -1 as that decimal
		// reads it rather than as a raw integer.
		let unit = 10i128.pow(u32::from(scale.max(0) as u8));
		return Ok(Arc::new(
			Decimal128Array::from_iter(
				input
					.iter()
					.map(|value| value.map(|value| i128::from(value.signum()) * unit)),
			)
			.with_precision_and_scale(precision, scale)?,
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("SIGN expects a numeric input, got {:?}", input.data_type()),
	})
}

/// A zero divisor has no remainder to report, so the row fails rather than
/// producing a value. `checked_rem` also covers `MIN % -1`, which overflows.
fn evaluate_mod(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let undefined = || ExecutionError::InvalidExpression {
		message: "MOD by zero is undefined".to_string(),
	};
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<Int32Array>(),
		right.as_any().downcast_ref::<Int32Array>(),
	) {
		let mut values = Vec::with_capacity(left.len());
		for (left, right) in left.iter().zip(right.iter()) {
			values.push(match (left, right) {
				(Some(left), Some(right)) => Some(left.checked_rem(right).ok_or_else(undefined)?),
				_ => None,
			});
		}
		return Ok(Arc::new(Int32Array::from_iter(values)) as ArrayRef);
	}
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<Int64Array>(),
		right.as_any().downcast_ref::<Int64Array>(),
	) {
		let mut values = Vec::with_capacity(left.len());
		for (left, right) in left.iter().zip(right.iter()) {
			values.push(match (left, right) {
				(Some(left), Some(right)) => Some(left.checked_rem(right).ok_or_else(undefined)?),
				_ => None,
			});
		}
		return Ok(Arc::new(Int64Array::from_iter(values)) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"MOD expects two arguments of the same integer type, got {:?} and {:?}",
			left.data_type(),
			right.data_type()
		),
	})
}

/// `ROUND` without a digit count is `ROUND(value, 0)`. The digit count is read
/// per row rather than required to be a literal, so a column of scales works;
/// a NULL count propagates like any other NULL input.
fn evaluate_round(value: &ArrayRef, digits: Option<&ArrayRef>) -> Result<ArrayRef, ExecutionError> {
	let digits = match digits {
		Some(digits) => Some(integer_digit_counts(digits)?),
		None => None,
	};
	let digit_at = |row: usize| -> Option<i32> {
		match &digits {
			Some(counts) => counts[row],
			None => Some(0),
		}
	};
	if let Some(value) = value.as_any().downcast_ref::<Float32Array>() {
		let rounded = (0..value.len())
			.map(
				|row| match (value.is_valid(row).then(|| value.value(row)), digit_at(row)) {
					(Some(value), Some(digits)) => {
						Some(round_to_digits(f64::from(value), digits) as f32)
					}
					_ => None,
				},
			)
			.collect::<Float32Array>();
		return Ok(Arc::new(rounded) as ArrayRef);
	}
	if let Some(value) = value.as_any().downcast_ref::<Float64Array>() {
		let rounded = (0..value.len())
			.map(
				|row| match (value.is_valid(row).then(|| value.value(row)), digit_at(row)) {
					(Some(value), Some(digits)) => Some(round_to_digits(value, digits)),
					_ => None,
				},
			)
			.collect::<Float64Array>();
		return Ok(Arc::new(rounded) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"ROUND expects Float32 or Float64 input, got {:?}",
			value.data_type()
		),
	})
}

fn integer_digit_counts(digits: &ArrayRef) -> Result<Vec<Option<i32>>, ExecutionError> {
	if let Some(digits) = digits.as_any().downcast_ref::<Int32Array>() {
		return Ok(digits.iter().collect());
	}
	if let Some(digits) = digits.as_any().downcast_ref::<Int64Array>() {
		return Ok(digits
			.iter()
			.map(|value| {
				value.map(|value| value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32)
			})
			.collect());
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"ROUND expects an integer digit count, got {:?}",
			digits.data_type()
		),
	})
}

/// Scaling by `10^digits` is what makes a digit count meaningful, but the scale
/// itself overflows to infinity well before the digit count does. Falling back
/// to the unscaled value keeps a large count from turning finite data into NaN.
fn round_to_digits(value: f64, digits: i32) -> f64 {
	let factor = 10f64.powi(digits);
	if !factor.is_finite() || factor == 0.0 {
		return value;
	}
	let scaled = value * factor;
	if !scaled.is_finite() {
		return value;
	}
	scaled.round() / factor
}

fn evaluate_unary_float(
	input: &ArrayRef,
	name: &'static str,
	float32: fn(f32) -> f32,
	float64: fn(f64) -> f64,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Float32Array>() {
		return Ok(Arc::new(Float32Array::from_iter(
			input.iter().map(|value| value.map(float32)),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float64Array>() {
		return Ok(Arc::new(Float64Array::from_iter(
			input.iter().map(|value| value.map(float64)),
		)) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"{name} expects Float32 or Float64 input, got {:?}",
			input.data_type()
		),
	})
}

fn evaluate_pow(base: &ArrayRef, exponent: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	if let (Some(base), Some(exponent)) = (
		base.as_any().downcast_ref::<Float32Array>(),
		exponent.as_any().downcast_ref::<Float32Array>(),
	) {
		return Ok(Arc::new(Float32Array::from_iter(
			base.iter()
				.zip(exponent.iter())
				.map(|(base, exponent)| Some(base?.powf(exponent?))),
		)) as ArrayRef);
	}
	if let (Some(base), Some(exponent)) = (
		base.as_any().downcast_ref::<Float64Array>(),
		exponent.as_any().downcast_ref::<Float64Array>(),
	) {
		return Ok(Arc::new(Float64Array::from_iter(
			base.iter()
				.zip(exponent.iter())
				.map(|(base, exponent)| Some(base?.powf(exponent?))),
		)) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"POW expects two arguments of the same float type, got {:?} and {:?}",
			base.data_type(),
			exponent.data_type()
		),
	})
}

fn evaluate_log(base: &ArrayRef, input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	if let (Some(base), Some(input)) = (
		base.as_any().downcast_ref::<Float32Array>(),
		input.as_any().downcast_ref::<Float32Array>(),
	) {
		return Ok(Arc::new(
			base.iter()
				.zip(input.iter())
				.map(|(base, input)| match (base, input) {
					(Some(base), Some(input)) => Some(input.log(base)),
					_ => None,
				})
				.collect::<Float32Array>(),
		) as ArrayRef);
	}
	if let (Some(base), Some(input)) = (
		base.as_any().downcast_ref::<Float64Array>(),
		input.as_any().downcast_ref::<Float64Array>(),
	) {
		return Ok(Arc::new(
			base.iter()
				.zip(input.iter())
				.map(|(base, input)| match (base, input) {
					(Some(base), Some(input)) => Some(input.log(base)),
					_ => None,
				})
				.collect::<Float64Array>(),
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"LOG expects two arguments of the same float type, got {:?} and {:?}",
			base.data_type(),
			input.data_type()
		),
	})
}

/// `ABS` preserves its input type, so the magnitude of the most negative
/// two's-complement value has no representation. That is an overflow, reported
/// the way every other checked integer path here reports one, never a silent
/// wrap back to the negative input.
fn evaluate_abs(input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let overflow = |data_type: &DataType| ExecutionError::InvalidExpression {
		message: format!("ABS overflows {data_type:?} for its most negative value"),
	};
	if let Some(input) = input.as_any().downcast_ref::<Int32Array>() {
		let mut values = Vec::with_capacity(input.len());
		for value in input.iter() {
			values.push(match value {
				Some(value) => Some(
					value
						.checked_abs()
						.ok_or_else(|| overflow(input.data_type()))?,
				),
				None => None,
			});
		}
		return Ok(Arc::new(Int32Array::from_iter(values)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Int64Array>() {
		let mut values = Vec::with_capacity(input.len());
		for value in input.iter() {
			values.push(match value {
				Some(value) => Some(
					value
						.checked_abs()
						.ok_or_else(|| overflow(input.data_type()))?,
				),
				None => None,
			});
		}
		return Ok(Arc::new(Int64Array::from_iter(values)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float32Array>() {
		return Ok(Arc::new(Float32Array::from_iter(
			input.iter().map(|value| value.map(f32::abs)),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float64Array>() {
		return Ok(Arc::new(Float64Array::from_iter(
			input.iter().map(|value| value.map(f64::abs)),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Decimal128Array>() {
		let (precision, scale) = match input.data_type() {
			DataType::Decimal128(precision, scale) => (*precision, *scale),
			other => {
				return Err(ExecutionError::InvalidExpression {
					message: format!("ABS expects a numeric input, got {other:?}"),
				});
			}
		};
		let mut values = Vec::with_capacity(input.len());
		for value in input.iter() {
			values.push(match value {
				Some(value) => Some(
					value
						.checked_abs()
						.ok_or_else(|| overflow(input.data_type()))?,
				),
				None => None,
			});
		}
		return Ok(Arc::new(
			Decimal128Array::from_iter(values).with_precision_and_scale(precision, scale)?,
		) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("ABS expects a numeric input, got {:?}", input.data_type()),
	})
}

fn evaluate_exp(input: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	if let Some(input) = input.as_any().downcast_ref::<Float32Array>() {
		return Ok(Arc::new(Float32Array::from_iter(
			input.iter().map(|value| value.map(|value| value.exp())),
		)) as ArrayRef);
	}
	if let Some(input) = input.as_any().downcast_ref::<Float64Array>() {
		return Ok(Arc::new(Float64Array::from_iter(
			input.iter().map(|value| value.map(|value| value.exp())),
		)) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"EXP expects Float32 or Float64 input, got {:?}",
			input.data_type()
		),
	})
}

fn evaluate_in_list(
	expr_arr: &ArrayRef,
	item_arrs: &[ArrayRef],
	negated: bool,
	num_rows: usize,
) -> Result<ArrayRef, ExecutionError> {
	let mut combined: Option<BooleanArray> = None;
	for item_arr in item_arrs {
		let mask = eq(expr_arr, item_arr)?;
		combined = Some(match combined {
			Some(current) => or_kleene(&current, &mask)?,
			None => mask,
		});
	}
	let mask = combined.unwrap_or_else(|| BooleanArray::from(vec![Some(false); num_rows]));
	if negated {
		Ok(Arc::new(not(&mask)?))
	} else {
		Ok(Arc::new(mask))
	}
}

/// SQL CASE reads like a lazy conditional, but this vectorized evaluator computes every branch
/// array eagerly and then zips by each WHEN mask. A hard-erroring kernel in a branch fails the
/// query even for rows where that branch is not selected. Integer div-by-zero is safe because
/// ADR 084 makes it yield NULL. Per-branch masked execution is a deferred follow-on; do not
/// treat CASE as a lazy error fence.
fn evaluate_case(
	when_then: &[(ArrayRef, ArrayRef)],
	else_array: ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let mut acc = else_array;
	for (condition, result) in when_then.iter().rev() {
		let mask = as_boolean(condition, "CASE WHEN condition")?;
		acc = arrow::compute::kernels::zip::zip(mask, &result.as_ref(), &acc.as_ref())?;
	}
	Ok(acc)
}

fn evaluate_like(
	expr_arr: &ArrayRef,
	pattern: &str,
	negated: bool,
	case_insensitive: bool,
) -> Result<ArrayRef, ExecutionError> {
	if expr_arr.data_type() != &DataType::Utf8 {
		return Err(ExecutionError::InvalidExpression {
			message: format!("LIKE expects Utf8 input, got {:?}", expr_arr.data_type()),
		});
	}
	let pattern = StringArray::new_scalar(pattern);
	let result = match (negated, case_insensitive) {
		(false, false) => arrow_like(expr_arr, &pattern)?,
		(true, false) => arrow_nlike(expr_arr, &pattern)?,
		(false, true) => arrow_ilike(expr_arr, &pattern)?,
		(true, true) => arrow_nilike(expr_arr, &pattern)?,
	};
	Ok(Arc::new(result) as ArrayRef)
}

/// Reads a bound that the binder already proved constant. Callers must handle
/// the empty batch first: there is no row 0 to read.
/// An interval literal materialises as a duration array, not an integer one.
/// Reading it as an integer would fail at run time for every bucket.
/// A constant string argument the binder already validated. An empty batch is
/// handled by the caller: there is no row 0 to read.
fn constant_string_argument(
	argument: &ArrayRef,
	role: &'static str,
) -> Result<String, ExecutionError> {
	argument
		.as_any()
		.downcast_ref::<StringArray>()
		.filter(|values| !values.is_empty() && values.is_valid(0))
		.map(|values| values.value(0).to_string())
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("internal: {role} is not a constant string after binding"),
		})
}

fn constant_interval_nanos(argument: &ArrayRef) -> Result<u64, ExecutionError> {
	let nanos = argument
		.as_any()
		.downcast_ref::<DurationNanosecondArray>()
		.filter(|values| values.is_valid(0))
		.map(|values| values.value(0))
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!(
				"internal: TIME_BUCKET interval is not a constant interval after binding, got {:?}",
				argument.data_type()
			),
		})?;
	u64::try_from(nanos).map_err(|_| ExecutionError::InvalidExpression {
		message: format!("TIME_BUCKET requires a positive interval, got {nanos}"),
	})
}

fn constant_integer_argument(
	argument: &ArrayRef,
	role: &'static str,
) -> Result<i64, ExecutionError> {
	if let Some(values) = argument.as_any().downcast_ref::<Int64Array>()
		&& values.is_valid(0)
	{
		return Ok(values.value(0));
	}
	if let Some(values) = argument.as_any().downcast_ref::<Int32Array>()
		&& values.is_valid(0)
	{
		return Ok(i64::from(values.value(0)));
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("internal: {role} is not a constant integer after binding"),
	})
}

fn evaluate_substring(
	input_arr: &ArrayRef,
	from: i64,
	length: Option<u64>,
) -> Result<ArrayRef, ExecutionError> {
	if from <= 0 {
		return Err(ExecutionError::InvalidExpression {
			message: "SUBSTRING FROM must be >= 1".to_string(),
		});
	}
	let Some(input) = input_arr.as_any().downcast_ref::<StringArray>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"SUBSTRING expects Utf8 input, got {:?}",
				input_arr.data_type()
			),
		});
	};
	let start = from - 1;
	let result = substring_by_char(input, start, length)?;
	Ok(Arc::new(result) as ArrayRef)
}

fn evaluate_at_time_zone(arr: &ArrayRef, zone: &str) -> Result<ArrayRef, ExecutionError> {
	parse_temporal_execution_zone(zone)?;
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
		return Ok(Arc::new(array.clone().with_timezone(zone.to_string())) as ArrayRef);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"AT TIME ZONE expects Timestamp input, got {:?}",
			arr.data_type()
		),
	})
}

/// Applies a fallible parse to every element of either string width.
///
/// The registration's failure property decides what an unparseable element
/// becomes, and it is the only thing that decides: a `YieldsNull` function
/// answers NULL for that row, a `Raises` function ends the statement. The value
/// itself never selects the behaviour, so a strict call cannot silently degrade
/// into a permissive one on unusual data.
fn parse_string_elements<T>(
	array: &ArrayRef,
	name: &str,
	failure: crate::ir::ScalarFunctionFailure,
	parse: impl Fn(&str) -> Result<T, String>,
) -> Result<Vec<Option<T>>, ExecutionError> {
	fn collect<'a, T>(
		values: impl Iterator<Item = Option<&'a str>>,
		name: &str,
		failure: crate::ir::ScalarFunctionFailure,
		parse: impl Fn(&str) -> Result<T, String>,
	) -> Result<Vec<Option<T>>, ExecutionError> {
		let mut parsed = Vec::new();
		for value in values {
			match value {
				None => parsed.push(None),
				Some(text) => match parse(text) {
					Ok(value) => parsed.push(Some(value)),
					Err(message) => match failure {
						crate::ir::ScalarFunctionFailure::YieldsNull => parsed.push(None),
						crate::ir::ScalarFunctionFailure::Raises => {
							return Err(ExecutionError::InvalidExpression {
								message: format!("{name}: {message}"),
							});
						}
					},
				},
			}
		}
		Ok(parsed)
	}

	if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
		return collect(array.iter(), name, failure, parse);
	}
	if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
		return collect(array.iter(), name, failure, parse);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("{name} expects a string input, got {:?}", array.data_type()),
	})
}

fn evaluate_to_timestamp_with_pattern(
	array: &ArrayRef,
	pattern: &crate::ir::TemporalFormatPattern,
	zone: &str,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let timezone = parse_temporal_execution_zone(zone)?;
	let parsed = parse_string_elements(array, "TO_TIMESTAMP", failure, |text| {
		let fields = crate::temporal_format::parse_fields(text, pattern)?;
		crate::temporal_format::fields_to_utc_nanos(&fields, timezone)
	})?;
	Ok(Arc::new(TimestampNanosecondArray::from(parsed).with_timezone("UTC")) as ArrayRef)
}

fn evaluate_to_char(
	array: &ArrayRef,
	pattern: &crate::ir::TemporalFormatPattern,
	zone: Option<&str>,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(dates) = array.as_any().downcast_ref::<Date32Array>() {
		let rendered = dates
			.iter()
			.map(|value| {
				value.map(|days| {
					crate::temporal_format::render_fields(
						&crate::temporal_format::date32_to_fields(days),
						pattern,
					)
				})
			})
			.collect::<StringArray>();
		return Ok(Arc::new(rendered) as ArrayRef);
	}
	let zone = zone.ok_or_else(|| ExecutionError::InvalidExpression {
		message: "bound TO_CHAR over a timestamp is missing its timezone".to_string(),
	})?;
	let timezone = parse_temporal_execution_zone(zone)?;
	let (values, scale): (Vec<Option<i64>>, i64) =
		if let Some(array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
			(array.iter().collect(), 1_000_000)
		} else if let Some(array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
			(array.iter().collect(), 1_000)
		} else if let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
			(array.iter().collect(), 1)
		} else {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"TO_CHAR expects a Timestamp or Date32 input, got {:?}",
					array.data_type()
				),
			});
		};
	let mut rows: Vec<Option<String>> = Vec::with_capacity(values.len());
	for value in values {
		let Some(ticks) = value else {
			rows.push(None);
			continue;
		};
		// Checked, not saturating. Clamping would render a different instant
		// than the value holds, which is a wrong answer rather than a failure.
		let nanos = ticks
			.checked_mul(scale)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!(
					"TO_CHAR: timestamp {ticks} is outside the representable nanosecond range"
				),
			})?;
		let fields = crate::temporal_format::utc_nanos_to_fields(nanos, timezone);
		rows.push(Some(crate::temporal_format::render_fields(
			&fields, pattern,
		)));
	}
	let rendered = rows.into_iter().collect::<StringArray>();
	Ok(Arc::new(rendered) as ArrayRef)
}

fn evaluate_to_timestamp(
	array: &ArrayRef,
	zone: &str,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let timezone = parse_temporal_execution_zone(zone)?;
	let parsed = parse_string_elements(array, "TO_TIMESTAMP", failure, |text| {
		crate::timestamp::parse_timestamp_text(text, timezone, TimeUnit::Nanosecond)
			.map_err(|error| error.to_string())
	})?;
	Ok(Arc::new(TimestampNanosecondArray::from(parsed).with_timezone("UTC")) as ArrayRef)
}

fn evaluate_to_date(
	array: &ArrayRef,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let parsed = parse_string_elements(array, "TO_DATE", failure, |text| {
		crate::date32::parse_date_ymd(text).map_err(|error| error.to_string())
	})?;
	Ok(Arc::new(Date32Array::from(parsed)) as ArrayRef)
}

fn evaluate_extract(
	field: TemporalExtractField,
	zone: Option<&str>,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(zone) = zone {
		let timezone = parse_temporal_execution_zone(zone)?;
		if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Millisecond, timezone, field);
		}
		if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Microsecond, timezone, field);
		}
		if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
			return timestamp_extract_array(array, TimeUnit::Nanosecond, timezone, field);
		}
		return Err(ExecutionError::InvalidExpression {
			message: format!("EXTRACT expects Timestamp input, got {:?}", arr.data_type()),
		});
	}
	let Some(array) = arr.as_any().downcast_ref::<Date32Array>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"Date32 EXTRACT expects Date32 input, got {:?}",
				arr.data_type()
			),
		});
	};
	if !field.is_date32_supported() {
		return Err(ExecutionError::InvalidExpression {
			message: format!("EXTRACT {} is not supported for Date32", field.as_str()),
		});
	}
	let mut builder = Int32Builder::with_capacity(array.len());
	for value in array.iter() {
		match value {
			Some(value) => {
				builder.append_value(crate::date32::date32_extract(value, field).map_err(
					|error| ExecutionError::InvalidExpression {
						message: error.to_string(),
					},
				)?)
			}
			None => builder.append_null(),
		}
	}
	Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn timestamp_extract_array<T>(
	array: &T,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
	field: TemporalExtractField,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let mut builder = Int32Builder::with_capacity(array.len());
	for value in array {
		match value {
			Some(value) => builder.append_value(
				crate::timestamp::timestamp_extract(value, unit, timezone, field).map_err(
					|error| ExecutionError::InvalidExpression {
						message: error.to_string(),
					},
				)?,
			),
			None => builder.append_null(),
		}
	}
	Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn evaluate_date_trunc(
	field: TemporalTruncField,
	zone: &str,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let timezone = parse_temporal_execution_zone(zone)?;
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Millisecond, timezone, field);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Microsecond, timezone, field);
	}
	if let Some(array) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
		return timestamp_trunc_array(array, TimeUnit::Nanosecond, timezone, field);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"date_trunc expects Timestamp input, got {:?}",
			arr.data_type()
		),
	})
}

fn timestamp_trunc_array<T>(
	array: &T,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
	field: TemporalTruncField,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let values = array
		.into_iter()
		.map(|value| {
			value
				.map(|value| {
					crate::timestamp::timestamp_trunc(value, unit, timezone, field).map_err(
						|error| ExecutionError::InvalidExpression {
							message: error.to_string(),
						},
					)
				})
				.transpose()
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	})
}

fn parse_temporal_execution_zone(zone: &str) -> Result<chrono_tz::Tz, ExecutionError> {
	crate::timestamp::parse_session_timezone(zone).map_err(|error| {
		ExecutionError::InvalidExpression {
			message: error.to_string(),
		}
	})
}

fn evaluate_time_bucket(
	interval: std::time::Duration,
	arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
		// Refused here, not only at lowering: an `Expr::TimeBucket` can be built
		// directly, and a zero interval previously reached this division and
		// aborted the process rather than returning an error.
		let interval_ms = i64::try_from(interval.as_millis())
			.ok()
			.filter(|millis| *millis > 0)
			.ok_or_else(|| ExecutionError::InvalidExpression {
				message: format!(
					"TIME_BUCKET requires a positive interval within i64 milliseconds, got {interval:?}"
				),
			})?;
		let bucketed: Vec<Option<i64>> = int_arr
			.iter()
			// `div_euclid` floors toward negative infinity. Plain `/` truncates
			// toward zero, which put every pre-epoch timestamp one bucket late:
			// with a 1s bucket, -1500 returned -1000 instead of -2000, and -1
			// shared the epoch bucket with +1.
			//
			// The product is checked because near `i64::MIN` the mathematical
			// bucket start falls below the representable range for every allowed
			// interval — a `1m` bucket of `i64::MIN` is -9_223_372_036_854_780_000.
			// Unchecked, that panics in debug and wraps to a positive timestamp in
			// release, which is the worse failure.
			.map(|value| {
				value
					.map(|value| {
						value
							.div_euclid(interval_ms)
							.checked_mul(interval_ms)
							.ok_or_else(|| ExecutionError::InvalidExpression {
								message: format!(
									"TIME_BUCKET bucket start for {value} at {interval_ms}ms is not representable in i64"
								),
							})
					})
					.transpose()
			})
			.collect::<Result<Vec<_>, _>>()?;
		Ok(Arc::new(Int64Array::from(bucketed)) as ArrayRef)
	} else {
		Err(ExecutionError::InvalidExpression {
			message: "TimeBucket expects Int64 (ms) input".to_string(),
		})
	}
}

fn evaluate_floor(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	match arr.data_type() {
		DataType::Int32 | DataType::Int64 => Ok(arr.clone()),
		DataType::Float32 => {
			let values = arr
				.as_any()
				.downcast_ref::<Float32Array>()
				.expect("Float32 data type must have Float32Array storage")
				.iter()
				.map(|value| match value {
					Some(value) if !value.is_finite() => Err(ExecutionError::Kernel {
						message: format!("FLOOR requires finite Float32 input: {value}"),
					}),
					Some(value) => Ok(Some(value.floor())),
					None => Ok(None),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Float32Array::from(values)))
		}
		DataType::Float64 => {
			let values = arr
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("Float64 data type must have Float64Array storage")
				.iter()
				.map(|value| match value {
					Some(value) if !value.is_finite() => Err(ExecutionError::Kernel {
						message: format!("FLOOR requires finite Float64 input: {value}"),
					}),
					Some(value) => Ok(Some(value.floor())),
					None => Ok(None),
				})
				.collect::<Result<Vec<_>, _>>()?;
			Ok(Arc::new(Float64Array::from(values)))
		}
		data_type => Err(ExecutionError::InvalidExpression {
			message: format!("FLOOR expects numeric input, got {data_type}"),
		}),
	}
}

fn evaluate_trunc(arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	match arr.data_type() {
		DataType::Int32 | DataType::Int64 => Ok(arr.clone()),
		DataType::Float32 => {
			let values = arr
				.as_any()
				.downcast_ref::<Float32Array>()
				.expect("Float32 data type must have Float32Array storage")
				.iter()
				.map(|value| value.map(f32::trunc))
				.collect::<Float32Array>();
			Ok(Arc::new(values) as ArrayRef)
		}
		DataType::Float64 => {
			let values = arr
				.as_any()
				.downcast_ref::<Float64Array>()
				.expect("Float64 data type must have Float64Array storage")
				.iter()
				.map(|value| value.map(f64::trunc))
				.collect::<Float64Array>();
			Ok(Arc::new(values) as ArrayRef)
		}
		data_type => Err(ExecutionError::InvalidExpression {
			message: format!("TRUNC expects numeric input, got {data_type}"),
		}),
	}
}

fn evaluate_decimal_division(
	left_arr: &ArrayRef,
	right_arr: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) =
		(left_arr.data_type(), right_arr.data_type())
	else {
		return Err(ExecutionError::InvalidExpression {
			message: "decimal division over non-decimal operands".to_string(),
		});
	};
	let (out_precision, out_scale) =
		decimal_division_output_type(*p1, *s1, *p2, *s2).ok_or_else(|| ExecutionError::Kernel {
			message: "decimal division output type out of range".to_string(),
		})?;
	let left = left_arr
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("checked Decimal128 numerator");
	let right = right_arr
		.as_any()
		.downcast_ref::<Decimal128Array>()
		.expect("checked Decimal128 denominator");
	let overflow = || ExecutionError::Kernel {
		message: "decimal division scale overflow".to_string(),
	};
	// `decimal_division_output_type` guarantees this exponent is in `0..=38`, so `10^scale_delta` fits i128.
	let scale_delta = i32::from(out_scale) + i32::from(*s2) - i32::from(*s1);
	let multiplier = 10_i128
		.checked_pow(u32::try_from(scale_delta).map_err(|_| overflow())?)
		.ok_or_else(overflow)?;
	let mut values: Vec<Option<i128>> = Vec::with_capacity(left.len());
	for index in 0..left.len() {
		if left.is_null(index) || right.is_null(index) {
			values.push(None);
			continue;
		}
		let divisor = right.value(index);
		if divisor == 0 {
			values.push(None); // ADR 084: div-by-zero -> NULL
			continue;
		}
		let scaled = left
			.value(index)
			.checked_mul(multiplier)
			.ok_or_else(overflow)?;
		let mut quotient = scaled / divisor;
		let remainder = scaled % divisor;
		if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
			quotient = quotient
				.checked_add(scaled.signum() * divisor.signum())
				.ok_or_else(|| ExecutionError::Kernel {
					message: "decimal division rounding overflow".to_string(),
				})?;
		}
		if !decimal_fits_precision(quotient, out_precision) {
			return Err(ExecutionError::Kernel {
				message: "decimal division result does not fit output precision".to_string(),
			});
		}
		values.push(Some(quotient));
	}
	Ok(Arc::new(
		Decimal128Array::from(values).with_precision_and_scale(out_precision, out_scale)?,
	))
}

pub(in crate::exec) fn evaluate_unbound_arithmetic(
	op: ArithmeticOp,
	left: ArrayRef,
	right: ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(result) = evaluate_temporal_arithmetic(op, &left, &right)? {
		return Ok(result);
	}
	let output_type =
		arithmetic_output_type(left.data_type(), right.data_type()).ok_or_else(|| {
			ExecutionError::InvalidExpression {
				message: format!(
					"arithmetic operands must be Int32, Int64, Float32, or Float64, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}
		})?;
	let left = if left.data_type() == &output_type {
		left
	} else {
		evaluate_cast(&left, &output_type)?
	};
	let right = if right.data_type() == &output_type {
		right
	} else {
		evaluate_cast(&right, &output_type)?
	};
	evaluate_arithmetic(op, &left, &right, &output_type)
}

/// ADR 564: text parses to a numeric per row, sharing ADR 558's per-element
/// failure machinery so strict `CAST` raises and `TRY_CAST` yields NULL
/// through the same loop. Surrounding ASCII whitespace is trimmed — the
/// standard's treatment; an integer target rejects fractional text rather
/// than truncating it.
fn cast_text_to_i32(
	array: &ArrayRef,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let parsed = parse_string_elements(array, "CAST to Int32", failure, |text| {
		text.trim()
			.parse::<i32>()
			.map_err(|_| format!("{text:?} is not a valid Int32"))
	})?;
	Ok(Arc::new(Int32Array::from(parsed)) as ArrayRef)
}

fn cast_text_to_i64(
	array: &ArrayRef,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let parsed = parse_string_elements(array, "CAST to Int64", failure, |text| {
		text.trim()
			.parse::<i64>()
			.map_err(|_| format!("{text:?} is not a valid Int64"))
	})?;
	Ok(Arc::new(Int64Array::from(parsed)) as ArrayRef)
}

fn cast_text_to_f32(
	array: &ArrayRef,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let parsed = parse_string_elements(array, "CAST to Float32", failure, |text| {
		text.trim()
			.parse::<f32>()
			.map_err(|_| format!("{text:?} is not a valid Float32"))
	})?;
	Ok(Arc::new(Float32Array::from(parsed)) as ArrayRef)
}

fn cast_text_to_f64(
	array: &ArrayRef,
	failure: crate::ir::ScalarFunctionFailure,
) -> Result<ArrayRef, ExecutionError> {
	let parsed = parse_string_elements(array, "CAST to Float64", failure, |text| {
		text.trim()
			.parse::<f64>()
			.map_err(|_| format!("{text:?} is not a valid Float64"))
	})?;
	Ok(Arc::new(Float64Array::from(parsed)) as ArrayRef)
}

/// ADR 564: one canonical text rendering per source family, matching what
/// results already print: decimal digits for integers, the shortest
/// round-trip form for floats, scale-faithful digits for decimals,
/// `YYYY-MM-DD` for dates, and the RFC3339 UTC form for timestamps.
/// Rendering is total, so `TRY_CAST` is identical by inheritance.
fn cast_to_utf8(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let any = array.as_any();
	let render = |values: Vec<Option<String>>| {
		Ok(Arc::new(values.into_iter().collect::<StringArray>()) as ArrayRef)
	};
	if let Some(array) = any.downcast_ref::<Int32Array>() {
		return render(array.iter().map(|v| v.map(|v| v.to_string())).collect());
	}
	if let Some(array) = any.downcast_ref::<Int64Array>() {
		return render(array.iter().map(|v| v.map(|v| v.to_string())).collect());
	}
	if let Some(array) = any.downcast_ref::<Float32Array>() {
		return render(array.iter().map(|v| v.map(|v| v.to_string())).collect());
	}
	if let Some(array) = any.downcast_ref::<Float64Array>() {
		return render(array.iter().map(|v| v.map(|v| v.to_string())).collect());
	}
	if let Some(array) = any.downcast_ref::<Decimal128Array>() {
		return render(
			(0..array.len())
				.map(|row| (!array.is_null(row)).then(|| array.value_as_string(row)))
				.collect(),
		);
	}
	if let Some(array) = any.downcast_ref::<Date32Array>() {
		return render(
			array
				.iter()
				.map(|v| v.map(crate::date32::format_date_ymd))
				.collect(),
		);
	}
	if let Some(array) = any.downcast_ref::<TimestampMillisecondArray>() {
		return render(
			array
				.iter()
				.map(|v| {
					v.map(|v| crate::timestamp::format_utc_timestamp(v, TimeUnit::Millisecond))
				})
				.collect(),
		);
	}
	if let Some(array) = any.downcast_ref::<TimestampMicrosecondArray>() {
		return render(
			array
				.iter()
				.map(|v| {
					v.map(|v| crate::timestamp::format_utc_timestamp(v, TimeUnit::Microsecond))
				})
				.collect(),
		);
	}
	if let Some(array) = any.downcast_ref::<TimestampNanosecondArray>() {
		return render(
			array
				.iter()
				.map(|v| v.map(|v| crate::timestamp::format_utc_timestamp(v, TimeUnit::Nanosecond)))
				.collect(),
		);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!("CAST to Utf8 is not supported from {:?}", array.data_type()),
	})
}

pub(crate) fn evaluate_cast(
	array: &ArrayRef,
	target: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	if array.data_type() == target {
		return Ok(array.clone());
	}

	match (array.data_type(), target) {
		// ADR 564: text parses strictly; a bad value raises naming itself.
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Int32) => {
			cast_text_to_i32(array, crate::ir::ScalarFunctionFailure::Raises)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Int64) => {
			cast_text_to_i64(array, crate::ir::ScalarFunctionFailure::Raises)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Float32) => {
			cast_text_to_f32(array, crate::ir::ScalarFunctionFailure::Raises)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Float64) => {
			cast_text_to_f64(array, crate::ir::ScalarFunctionFailure::Raises)
		}
		// ADR 564: rendering to text is canonical and total.
		(_, DataType::Utf8) => cast_to_utf8(array),
		(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
		(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
		(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
		(DataType::Int64, DataType::Int32) => cast_i64_to_i32(array),
		(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
		(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
		(DataType::Float32, DataType::Int32) => cast_f32_to_i32(array),
		(DataType::Float32, DataType::Int64) => cast_f32_to_i64(array),
		(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
		(DataType::Float64, DataType::Int32) => cast_f64_to_i32(array),
		(DataType::Float64, DataType::Int64) => cast_f64_to_i64(array),
		(DataType::Float64, DataType::Float32) => cast_f64_to_f32(array),
		(DataType::Int32, DataType::Decimal128(_, _))
		| (DataType::Int64, DataType::Decimal128(_, _)) => arrow::compute::cast(array, target)
			.map_err(|error| cast_failed(array.data_type(), target, error)),
		(DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
			arrow::compute::cast(array, target)
				.map_err(|error| cast_failed(array.data_type(), target, error))
		}
		(source, target) => Err(cast_failed(
			source,
			target,
			"unsupported combination (binder gate failure)",
		)),
	}
}

fn evaluate_try_cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ExecutionError> {
	if array.data_type() == target {
		return Ok(array.clone());
	}

	match (array.data_type(), target) {
		// ADR 564: the TRY spelling yields NULL for an unparseable value,
		// through the same per-element loop the strict arm raises from.
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Int32) => {
			cast_text_to_i32(array, crate::ir::ScalarFunctionFailure::YieldsNull)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Int64) => {
			cast_text_to_i64(array, crate::ir::ScalarFunctionFailure::YieldsNull)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Float32) => {
			cast_text_to_f32(array, crate::ir::ScalarFunctionFailure::YieldsNull)
		}
		(DataType::Utf8 | DataType::LargeUtf8, DataType::Float64) => {
			cast_text_to_f64(array, crate::ir::ScalarFunctionFailure::YieldsNull)
		}
		// Rendering is total, so the TRY form is the strict form.
		(_, DataType::Utf8) => cast_to_utf8(array),
		(DataType::Int32, DataType::Int64) => cast_i32_to_i64(array),
		(DataType::Int32, DataType::Float32) => cast_i32_to_f32(array),
		(DataType::Int32, DataType::Float64) => cast_i32_to_f64(array),
		(DataType::Int64, DataType::Float32) => cast_i64_to_f32(array),
		(DataType::Int64, DataType::Float64) => cast_i64_to_f64(array),
		(DataType::Float32, DataType::Float64) => cast_f32_to_f64(array),
		(DataType::Int64, DataType::Int32) => try_cast_i64_to_i32(array),
		(DataType::Float64, DataType::Float32) => try_cast_f64_to_f32(array),
		(DataType::Float32, DataType::Int32) => try_cast_f32_to_i32(array),
		(DataType::Float32, DataType::Int64) => try_cast_f32_to_i64(array),
		(DataType::Float64, DataType::Int32) => try_cast_f64_to_i32(array),
		(DataType::Float64, DataType::Int64) => try_cast_f64_to_i64(array),
		(source, target) => Err(cast_failed(
			source,
			target,
			"unsupported combination (binder gate failure)",
		)),
	}
}

fn cast_failed(source: &DataType, target: &DataType, cause: impl fmt::Display) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("cast failed: {source:?} to {target:?}: {cause}"),
	}
}

fn cast_i32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(i64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_i32_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f32))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_i32_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(f64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_i64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value < i32::MIN as i64 || value > i32::MAX as i64 {
				return Err(cast_failed(
					&DataType::Int64,
					&DataType::Int32,
					format!("value {value} does not fit"),
				));
			}
			Ok(Some(value as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_i64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f32))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn cast_i64_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|v| v.map(|v| v as f64))
		.collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_f32_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					"Inf has no integer image",
				));
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int32,
					format!("value {value} truncated to {trunc} does not fit Int32"),
				));
			}
			Ok(Some(trunc as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_f32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i64>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					"Inf has no integer image",
				));
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float32,
					&DataType::Int64,
					format!("value {value} truncated to {trunc} does not fit Int64"),
				));
			}
			Ok(Some(trunc as i64))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_f32_to_f64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "Cast source")?;
	let values = array.iter().map(|v| v.map(f64::from)).collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					"Inf has no integer image",
				));
			}
			let trunc = value.trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int32,
					format!("value {value} truncated to {trunc} does not fit Int32"),
				));
			}
			Ok(Some(trunc as i32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn cast_f64_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<i64>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_nan() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					"NaN has no integer image",
				));
			}
			if value.is_infinite() {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					"Inf has no integer image",
				));
			}
			let trunc = value.trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Int64,
					format!("value {value} truncated to {trunc} does not fit Int64"),
				));
			}
			Ok(Some(trunc as i64))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn cast_f64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "Cast source")?;
	let values = array
		.iter()
		.map(|value| -> Result<Option<f32>, ExecutionError> {
			let Some(value) = value else {
				return Ok(None);
			};
			if value.is_finite() && value.abs() > f32::MAX as f64 {
				return Err(cast_failed(
					&DataType::Float64,
					&DataType::Float32,
					format!("finite value {value} overflows Float32"),
				));
			}
			Ok(Some(value as f32))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn try_cast_i64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_int64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value < i32::MIN as i64 || value > i32::MAX as i64 {
				return None;
			}
			Some(value as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_f32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<f32> {
			let value = value?;
			if value.is_finite() && value.abs() > f32::MAX as f64 {
				return None;
			}
			Some(value as f32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn try_cast_f32_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f32_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float32(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i64> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = f64::from(value).trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i64)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_i32(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i32> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = value.trunc();
			if !(F64_I32_LOWER..F64_I32_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i32)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn try_cast_f64_to_i64(array: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
	let array = as_float64(array, "TryCast source")?;
	let values = array
		.iter()
		.map(|value| -> Option<i64> {
			let value = value?;
			if value.is_nan() {
				return None;
			}
			if value.is_infinite() {
				return None;
			}
			let trunc = value.trunc();
			if !(F64_I64_LOWER..F64_I64_UPPER).contains(&trunc) {
				return None;
			}
			Some(trunc as i64)
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn evaluate_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
	output_type: &DataType,
) -> Result<ArrayRef, ExecutionError> {
	if left.len() != right.len() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"arithmetic operand length mismatch: left {}, right {}",
				left.len(),
				right.len()
			),
		});
	}
	if left.data_type() != output_type || right.data_type() != output_type {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"arithmetic operands must already be {:?}, got {:?} and {:?}",
				output_type,
				left.data_type(),
				right.data_type()
			),
		});
	}

	match output_type {
		DataType::Int32 => evaluate_int32_arithmetic(op, left, right),
		DataType::Int64 => evaluate_int64_arithmetic(op, left, right),
		DataType::Float32 => evaluate_float32_arithmetic(op, left, right),
		DataType::Float64 => evaluate_float64_arithmetic(op, left, right),
		other => Err(ExecutionError::InvalidExpression {
			message: format!("unsupported arithmetic output type: {other:?}"),
		}),
	}
}

fn evaluate_temporal_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<Option<ArrayRef>, ExecutionError> {
	let left_is_calendar = is_calendar_month_interval_type_exec(left.data_type());
	let right_is_calendar = is_calendar_month_interval_type_exec(right.data_type());
	if left_is_calendar || right_is_calendar {
		if left.len() != right.len() {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"temporal arithmetic operand length mismatch: left {}, right {}",
					left.len(),
					right.len()
				),
			});
		}
		return Ok(Some(evaluate_calendar_arithmetic(op, left, right)?));
	}
	let left_is_date32 = left.data_type() == &DataType::Date32;
	let right_is_date32 = right.data_type() == &DataType::Date32;
	let left_is_interval = left.data_type() == &DataType::Duration(TimeUnit::Nanosecond);
	let right_is_interval = right.data_type() == &DataType::Duration(TimeUnit::Nanosecond);
	let left_is_integer = matches!(left.data_type(), DataType::Int32 | DataType::Int64);
	let right_is_integer = matches!(right.data_type(), DataType::Int32 | DataType::Int64);
	if (left_is_date32 || right_is_date32)
		&& (left_is_interval
			|| right_is_interval
			|| left_is_integer
			|| right_is_integer
			|| (left_is_date32 && right_is_date32))
	{
		if left.len() != right.len() {
			return Err(ExecutionError::InvalidExpression {
				message: format!(
					"temporal arithmetic operand length mismatch: left {}, right {}",
					left.len(),
					right.len()
				),
			});
		}
		if left_is_date32 && right_is_interval {
			return Ok(Some(evaluate_date32_fixed_interval(op, left, right)?));
		}
		if left_is_interval && right_is_date32 {
			if matches!(op, ArithmeticOp::Add) {
				return Ok(Some(evaluate_date32_fixed_interval(op, right, left)?));
			}
			return Err(ExecutionError::InvalidExpression {
				message: "Interval - Date32 is not supported".to_string(),
			});
		}
		if left_is_date32 && right_is_date32 {
			if matches!(op, ArithmeticOp::Sub) {
				return Ok(Some(evaluate_date32_difference(left, right)?));
			}
			return Err(ExecutionError::InvalidExpression {
				message: "Date32 + Date32 is not supported".to_string(),
			});
		}
		if left_is_date32 && right_is_integer {
			return Ok(Some(evaluate_date32_integer(op, left, right)?));
		}
		if left_is_integer && right_is_date32 {
			if matches!(op, ArithmeticOp::Add) {
				return Ok(Some(evaluate_date32_integer(op, right, left)?));
			}
			return Err(ExecutionError::InvalidExpression {
				message: "integer - Date32 is not supported".to_string(),
			});
		}
	}
	let left_unit = temporal_timestamp_unit(left.data_type())?;
	let right_unit = temporal_timestamp_unit(right.data_type())?;
	if left_unit.is_none() && right_unit.is_none() && !left_is_interval && !right_is_interval {
		return Ok(None);
	}
	if left.len() != right.len() {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"temporal arithmetic operand length mismatch: left {}, right {}",
				left.len(),
				right.len()
			),
		});
	}
	if (left_is_interval || right_is_interval)
		&& matches!(op, ArithmeticOp::Mul | ArithmeticOp::Div)
	{
		return Err(ExecutionError::InvalidExpression {
			message: "Interval multiplication and division are deferred".to_string(),
		});
	}

	match op {
		ArithmeticOp::Add => match (left_unit, right_unit, left_is_interval, right_is_interval) {
			(Some(unit), None, false, true) => {
				let durations = as_duration_nanos(right, "Timestamp + Interval right operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, left, durations, unit,
				)?))
			}
			(None, Some(unit), true, false) => {
				let durations = as_duration_nanos(left, "Interval + Timestamp left operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, right, durations, unit,
				)?))
			}
			(None, None, true, true) => Ok(Some(evaluate_interval_interval(op, left, right)?)),
			(Some(_), Some(_), _, _) => Err(ExecutionError::InvalidExpression {
				message: "Timestamp + Timestamp is not supported".to_string(),
			}),
			_ => Err(ExecutionError::InvalidExpression {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}),
		},
		ArithmeticOp::Sub => match (left_unit, right_unit, left_is_interval, right_is_interval) {
			(Some(unit), None, false, true) => {
				let durations = as_duration_nanos(right, "Timestamp - Interval right operand")?;
				Ok(Some(evaluate_timestamp_interval(
					op, left, durations, unit,
				)?))
			}
			(Some(left_unit), Some(right_unit), _, _) => {
				if left_unit != right_unit {
					return Err(ExecutionError::InvalidExpression {
						message: "mixed-unit Timestamp - Timestamp is deferred in ADR 152"
							.to_string(),
					});
				}
				Ok(Some(evaluate_timestamp_difference(left, right, left_unit)?))
			}
			(None, Some(_), true, false) => Err(ExecutionError::InvalidExpression {
				message: "Interval - Timestamp is not supported".to_string(),
			}),
			(None, None, true, true) => Ok(Some(evaluate_interval_interval(op, left, right)?)),
			_ => Err(ExecutionError::InvalidExpression {
				message: format!(
					"Interval arithmetic expects Interval and UTC Timestamp operands, got {:?} and {:?}",
					left.data_type(),
					right.data_type()
				),
			}),
		},
		ArithmeticOp::Mul | ArithmeticOp::Div => Ok(None),
	}
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

fn evaluate_date32_fixed_interval(
	op: ArithmeticOp,
	dates: &ArrayRef,
	intervals: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let dates = dates
		.as_any()
		.downcast_ref::<Date32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!(
				"Date32 +/- Interval expects Date32 input, got {:?}",
				dates.data_type()
			),
		})?;
	let intervals = as_duration_nanos(intervals, "Date32 +/- Interval operand")?;
	let values = dates
		.iter()
		.zip(intervals.iter())
		.map(|(days, nanos)| {
			let Some(days) = days else { return Ok(None) };
			let Some(nanos) = nanos else { return Ok(None) };
			if nanos % NANOS_PER_DAY != 0 {
				return Err(ExecutionError::InvalidExpression {
					message: format!(
						"Date32 +/- Interval requires a whole number of days; got {nanos} nanoseconds"
					),
				});
			}
			let delta = nanos / NANOS_PER_DAY;
			let result = match op {
				ArithmeticOp::Add => i128::from(days) + i128::from(delta),
				ArithmeticOp::Sub => i128::from(days) - i128::from(delta),
				_ => {
					return Err(ExecutionError::InvalidExpression {
						message: "Date32 interval multiplication and division are not supported"
							.to_string(),
					});
				}
			};
			let result = i32::try_from(result)
				.map_err(|_| temporal_arithmetic_overflow_exec(op, &DataType::Date32))?;
			Ok(Some(result))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

fn evaluate_date32_integer(
	op: ArithmeticOp,
	dates: &ArrayRef,
	integers: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let dates = dates
		.as_any()
		.downcast_ref::<Date32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!(
				"Date32 integer arithmetic expects Date32 input, got {:?}",
				dates.data_type()
			),
		})?;
	let values = if let Some(integers) = integers.as_any().downcast_ref::<Int32Array>() {
		dates
			.iter()
			.zip(integers.iter())
			.map(|(days, delta)| date32_integer_value(op, days, delta.map(i128::from)))
			.collect::<Result<Vec<_>, ExecutionError>>()?
	} else if let Some(integers) = integers.as_any().downcast_ref::<Int64Array>() {
		dates
			.iter()
			.zip(integers.iter())
			.map(|(days, delta)| date32_integer_value(op, days, delta.map(i128::from)))
			.collect::<Result<Vec<_>, ExecutionError>>()?
	} else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"Date32 integer operand must be Int32 or Int64, got {:?}",
				integers.data_type()
			),
		});
	};
	Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

fn date32_integer_value(
	op: ArithmeticOp,
	days: Option<i32>,
	delta: Option<i128>,
) -> Result<Option<i32>, ExecutionError> {
	let Some(days) = days else { return Ok(None) };
	let Some(delta) = delta else { return Ok(None) };
	let result = match op {
		ArithmeticOp::Add => i128::from(days) + delta,
		ArithmeticOp::Sub => i128::from(days) - delta,
		_ => {
			return Err(ExecutionError::InvalidExpression {
				message: "Date32 integer multiplication and division are not supported".to_string(),
			});
		}
	};
	i32::try_from(result)
		.map(Some)
		.map_err(|_| temporal_arithmetic_overflow_exec(op, &DataType::Date32))
}

fn evaluate_date32_difference(
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = left
		.as_any()
		.downcast_ref::<Date32Array>()
		.expect("Date32 difference left operand");
	let right = right
		.as_any()
		.downcast_ref::<Date32Array>()
		.expect("Date32 difference right operand");
	let values = left
		.iter()
		.zip(right.iter())
		.map(|(left, right)| {
			let Some(left) = left else { return Ok(None) };
			let Some(right) = right else { return Ok(None) };
			i32::try_from(i128::from(left) - i128::from(right))
				.map(Some)
				.map_err(|_| temporal_arithmetic_overflow_exec(ArithmeticOp::Sub, &DataType::Int32))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn evaluate_calendar_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left_timestamp = calendar_timestamp_context(left.data_type())?;
	let right_timestamp = calendar_timestamp_context(right.data_type())?;
	let left_is_calendar = is_calendar_month_interval_type_exec(left.data_type());
	let right_is_calendar = is_calendar_month_interval_type_exec(right.data_type());
	let left_is_date32 = matches!(left.data_type(), DataType::Date32);
	let right_is_date32 = matches!(right.data_type(), DataType::Date32);

	if (left_is_calendar || right_is_calendar)
		&& matches!(op, ArithmeticOp::Mul | ArithmeticOp::Div)
	{
		return Err(ExecutionError::InvalidExpression {
			message: "CalendarMonthInterval multiplication and division are deferred".to_string(),
		});
	}
	if (left_is_calendar && right.data_type() == &DataType::Duration(TimeUnit::Nanosecond))
		|| (left.data_type() == &DataType::Duration(TimeUnit::Nanosecond) && right_is_calendar)
	{
		return Err(ExecutionError::InvalidExpression {
			message:
				"CalendarMonthInterval and fixed-duration Interval arithmetic is not supported"
					.to_string(),
		});
	}

	match op {
		ArithmeticOp::Add => match (
			left_timestamp,
			right_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(Some((unit, timezone)), None, false, false, false, true) => {
				let intervals = as_interval_year_month(
					right,
					"Timestamp + CalendarMonthInterval right operand",
				)?;
				evaluate_timestamp_calendar_interval(op, left, intervals, unit, timezone)
			}
			(None, Some((unit, timezone)), false, false, true, false) => {
				let intervals =
					as_interval_year_month(left, "CalendarMonthInterval + Timestamp left operand")?;
				evaluate_timestamp_calendar_interval(op, right, intervals, unit, timezone)
			}
			(None, None, true, false, false, true) => {
				let intervals =
					as_interval_year_month(right, "Date32 + CalendarMonthInterval right operand")?;
				evaluate_date32_calendar_interval(op, left, intervals)
			}
			(None, None, false, true, true, false) => {
				let intervals =
					as_interval_year_month(left, "CalendarMonthInterval + Date32 left operand")?;
				evaluate_date32_calendar_interval(op, right, intervals)
			}
			(None, None, false, false, true, true) => {
				evaluate_calendar_interval_interval(op, left, right)
			}
			_ => Err(calendar_arithmetic_mismatch_exec(
				left.data_type(),
				right.data_type(),
			)),
		},
		ArithmeticOp::Sub => match (
			left_timestamp,
			right_timestamp,
			left_is_date32,
			right_is_date32,
			left_is_calendar,
			right_is_calendar,
		) {
			(Some((unit, timezone)), None, false, false, false, true) => {
				let intervals = as_interval_year_month(
					right,
					"Timestamp - CalendarMonthInterval right operand",
				)?;
				evaluate_timestamp_calendar_interval(op, left, intervals, unit, timezone)
			}
			(None, None, true, false, false, true) => {
				let intervals =
					as_interval_year_month(right, "Date32 - CalendarMonthInterval right operand")?;
				evaluate_date32_calendar_interval(op, left, intervals)
			}
			(None, None, false, false, true, true) => {
				evaluate_calendar_interval_interval(op, left, right)
			}
			(None, Some(_), false, false, true, false) => Err(ExecutionError::InvalidExpression {
				message: "CalendarMonthInterval - Timestamp is not supported".to_string(),
			}),
			_ => Err(calendar_arithmetic_mismatch_exec(
				left.data_type(),
				right.data_type(),
			)),
		},
		ArithmeticOp::Mul | ArithmeticOp::Div => Err(ExecutionError::InvalidExpression {
			message: "CalendarMonthInterval multiplication and division are deferred".to_string(),
		}),
	}
}

fn calendar_timestamp_context(
	data_type: &DataType,
) -> Result<Option<(TimeUnit, chrono_tz::Tz)>, ExecutionError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(zone),
		) => Ok(Some((*unit, parse_temporal_execution_zone(zone.as_ref())?))),
		DataType::Timestamp(_, None) => Err(ExecutionError::InvalidExpression {
			message: "calendar interval arithmetic does not support Timestamp without timezone"
				.to_string(),
		}),
		DataType::Timestamp(unit, Some(_)) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"calendar interval arithmetic does not support Timestamp unit {unit:?}"
			),
		}),
		_ => Ok(None),
	}
}

fn is_calendar_month_interval_type_exec(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Interval(IntervalUnit::YearMonth))
}

fn evaluate_timestamp_calendar_interval(
	op: ArithmeticOp,
	timestamps: &ArrayRef,
	intervals: &IntervalYearMonthArray,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMicrosecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampNanosecondArray>()
	{
		return timestamp_calendar_interval_array(op, array, intervals, unit, timezone);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp +/- CalendarMonthInterval expects Timestamp input, got {:?}",
			timestamps.data_type()
		),
	})
}

fn timestamp_calendar_interval_array<T>(
	op: ArithmeticOp,
	timestamps: &T,
	intervals: &IntervalYearMonthArray,
	unit: TimeUnit,
	timezone: chrono_tz::Tz,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let data_type = timestamps.data_type().clone();
	let values = timestamps
		.into_iter()
		.zip(intervals.iter())
		.map(|(timestamp, months)| {
			let Some(timestamp) = timestamp else {
				return Ok(None);
			};
			let Some(months) = months else {
				return Ok(None);
			};
			let months = match op {
				ArithmeticOp::Add => months,
				ArithmeticOp::Sub => negate_calendar_months_exec(months, op, &data_type)?,
				ArithmeticOp::Mul | ArithmeticOp::Div => {
					return Err(ExecutionError::InvalidExpression {
						message: "CalendarMonthInterval multiplication and division are deferred"
							.to_string(),
					});
				}
			};
			crate::timestamp::add_calendar_months(timestamp, unit, timezone, months)
				.map(Some)
				.map_err(|error| ExecutionError::InvalidExpression {
					message: error.to_string(),
				})
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(timestamp_array_from_values(unit, values))
}

fn evaluate_date32_calendar_interval(
	op: ArithmeticOp,
	dates: &ArrayRef,
	intervals: &IntervalYearMonthArray,
) -> Result<ArrayRef, ExecutionError> {
	let Some(dates) = dates.as_any().downcast_ref::<Date32Array>() else {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"Date32 +/- CalendarMonthInterval expects Date32 input, got {:?}",
				dates.data_type()
			),
		});
	};
	let data_type = dates.data_type().clone();
	let values = dates
		.iter()
		.zip(intervals.iter())
		.map(|(days, months)| {
			let Some(days) = days else {
				return Ok(None);
			};
			let Some(months) = months else {
				return Ok(None);
			};
			let months = match op {
				ArithmeticOp::Add => months,
				ArithmeticOp::Sub => negate_calendar_months_exec(months, op, &data_type)?,
				ArithmeticOp::Mul | ArithmeticOp::Div => {
					return Err(ExecutionError::InvalidExpression {
						message: "CalendarMonthInterval multiplication and division are deferred"
							.to_string(),
					});
				}
			};
			crate::date32::add_calendar_months(days, months)
				.map(Some)
				.map_err(|error| ExecutionError::InvalidExpression {
					message: error.to_string(),
				})
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

fn evaluate_calendar_interval_interval(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_interval_year_month(left, "CalendarMonthInterval arithmetic left operand")?;
	let right = as_interval_year_month(right, "CalendarMonthInterval arithmetic right operand")?;
	let data_type = DataType::Interval(IntervalUnit::YearMonth);
	let values = left
		.iter()
		.zip(right.iter())
		.map(|(left, right)| {
			let Some(left) = left else {
				return Ok(None);
			};
			let Some(right) = right else {
				return Ok(None);
			};
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| temporal_arithmetic_overflow_exec(op, &data_type))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(IntervalYearMonthArray::from(values)) as ArrayRef)
}

fn negate_calendar_months_exec(
	months: i32,
	op: ArithmeticOp,
	data_type: &DataType,
) -> Result<i32, ExecutionError> {
	months
		.checked_neg()
		.ok_or_else(|| temporal_arithmetic_overflow_exec(op, data_type))
}

fn calendar_arithmetic_mismatch_exec(left: &DataType, right: &DataType) -> ExecutionError {
	ExecutionError::InvalidExpression {
		message: format!(
			"CalendarMonthInterval arithmetic expects Timestamp, Date32, or CalendarMonthInterval operands, got {left:?} and {right:?}"
		),
	}
}

fn temporal_timestamp_unit(data_type: &DataType) -> Result<Option<TimeUnit>, ExecutionError> {
	match data_type {
		DataType::Timestamp(
			unit @ (TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond),
			Some(timezone),
		) if timezone.as_ref() == "UTC" => Ok(Some(*unit)),
		DataType::Timestamp(_, None) => Err(ExecutionError::InvalidExpression {
			message: "temporal interval arithmetic does not support Timestamp without timezone"
				.to_string(),
		}),
		DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() != "UTC" => {
			Err(ExecutionError::InvalidExpression {
				message: format!(
					"temporal interval arithmetic requires UTC Timestamp, got timezone {timezone}"
				),
			})
		}
		DataType::Timestamp(unit, Some(_)) => Err(ExecutionError::InvalidExpression {
			message: format!(
				"temporal interval arithmetic does not support Timestamp unit {unit:?}"
			),
		}),
		_ => Ok(None),
	}
}

fn evaluate_timestamp_interval(
	op: ArithmeticOp,
	timestamps: &ArrayRef,
	durations: &DurationNanosecondArray,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError> {
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMillisecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampMicrosecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	if let Some(array) = timestamps
		.as_any()
		.downcast_ref::<TimestampNanosecondArray>()
	{
		return timestamp_interval_array(op, array, durations, unit);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp +/- Interval expects Timestamp input, got {:?}",
			timestamps.data_type()
		),
	})
}

fn timestamp_interval_array<T>(
	op: ArithmeticOp,
	timestamps: &T,
	durations: &DurationNanosecondArray,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let values = timestamps
		.into_iter()
		.zip(durations.iter())
		.map(|(timestamp, interval_nanos)| {
			let Some(timestamp) = timestamp else {
				return Ok(None);
			};
			let Some(interval_nanos) = interval_nanos else {
				return Ok(None);
			};
			let interval_ticks =
				checked_interval_nanos_to_timestamp_ticks_exec(interval_nanos, unit)?;
			let value = match op {
				ArithmeticOp::Add => timestamp.checked_add(interval_ticks),
				ArithmeticOp::Sub => timestamp.checked_sub(interval_ticks),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| temporal_arithmetic_overflow_exec(op, timestamps.data_type()))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(timestamp_array_from_values(unit, values))
}

fn evaluate_timestamp_difference(
	left: &ArrayRef,
	right: &ArrayRef,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError> {
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampMillisecondArray>(),
		right.as_any().downcast_ref::<TimestampMillisecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampMicrosecondArray>(),
		right.as_any().downcast_ref::<TimestampMicrosecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	if let (Some(left), Some(right)) = (
		left.as_any().downcast_ref::<TimestampNanosecondArray>(),
		right.as_any().downcast_ref::<TimestampNanosecondArray>(),
	) {
		return timestamp_difference_array(left, right, unit);
	}
	Err(ExecutionError::InvalidExpression {
		message: format!(
			"Timestamp - Timestamp expects matching timestamp arrays, got {:?} and {:?}",
			left.data_type(),
			right.data_type()
		),
	})
}

fn timestamp_difference_array<T>(
	left: &T,
	right: &T,
	unit: TimeUnit,
) -> Result<ArrayRef, ExecutionError>
where
	for<'a> &'a T: IntoIterator<Item = Option<i64>>,
	T: Array,
{
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	let data_type = left.data_type().clone();
	let values = left
		.into_iter()
		.zip(right.into_iter())
		.map(|(left_value, right_value)| {
			let Some(left_value) = left_value else {
				return Ok(None);
			};
			let Some(right_value) = right_value else {
				return Ok(None);
			};
			let ticks = left_value
				.checked_sub(right_value)
				.ok_or_else(|| temporal_arithmetic_overflow_exec(ArithmeticOp::Sub, &data_type))?;
			let nanos = ticks.checked_mul(nanos_per_unit).ok_or_else(|| {
				temporal_arithmetic_overflow_exec(
					ArithmeticOp::Sub,
					&DataType::Duration(TimeUnit::Nanosecond),
				)
			})?;
			Ok(Some(nanos))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(DurationNanosecondArray::from(values)) as ArrayRef)
}

fn evaluate_interval_interval(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_duration_nanos(left, "Interval arithmetic left operand")?;
	let right = as_duration_nanos(right, "Interval arithmetic right operand")?;
	let values = left
		.iter()
		.zip(right.iter())
		.map(|(left, right)| {
			let Some(left) = left else {
				return Ok(None);
			};
			let Some(right) = right else {
				return Ok(None);
			};
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul | ArithmeticOp::Div => None,
			}
			.ok_or_else(|| {
				temporal_arithmetic_overflow_exec(op, &DataType::Duration(TimeUnit::Nanosecond))
			})?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, ExecutionError>>()?;
	Ok(Arc::new(DurationNanosecondArray::from(values)) as ArrayRef)
}

fn timestamp_array_from_values(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
	match unit {
		TimeUnit::Millisecond => {
			Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Microsecond => {
			Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Nanosecond => {
			Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC")) as ArrayRef
		}
		TimeUnit::Second => unreachable!("TimestampSecond is not a supported Toron timestamp unit"),
	}
}

fn checked_interval_nanos_to_timestamp_ticks_exec(
	interval_nanos: i64,
	unit: TimeUnit,
) -> Result<i64, ExecutionError> {
	let nanos_per_unit =
		nanos_per_timestamp_unit(unit).ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("Timestamp unit {unit:?} is not supported for interval arithmetic"),
		})?;
	if interval_nanos % nanos_per_unit != 0 {
		return Err(ExecutionError::InvalidExpression {
			message: format!(
				"interval nanoseconds {interval_nanos} are not exactly representable at {} precision",
				timestamp_precision_name(unit)
			),
		});
	}
	Ok(interval_nanos / nanos_per_unit)
}

fn timestamp_precision_name(unit: TimeUnit) -> &'static str {
	match unit {
		TimeUnit::Second => "second",
		TimeUnit::Millisecond => "millisecond",
		TimeUnit::Microsecond => "microsecond",
		TimeUnit::Nanosecond => "nanosecond",
	}
}

fn temporal_arithmetic_overflow_exec(op: ArithmeticOp, data_type: &DataType) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!("temporal arithmetic overflow: {}({data_type:?})", op.name()),
	}
}

fn evaluate_int32_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_int32(left, "Int32 arithmetic left operand")?;
	let right = as_int32(right, "Int32 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| -> Result<Option<i32>, ExecutionError> {
			if left.is_null(row) || right.is_null(row) {
				return Ok(None);
			}
			let left = left.value(row);
			let right = right.value(row);
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul => left.checked_mul(right),
				ArithmeticOp::Div => {
					if right == 0 {
						return Ok(None);
					}
					left.checked_div(right)
				}
			}
			.ok_or_else(|| arithmetic_overflow(op, &DataType::Int32))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
}

fn evaluate_int64_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_int64(left, "Int64 arithmetic left operand")?;
	let right = as_int64(right, "Int64 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| -> Result<Option<i64>, ExecutionError> {
			if left.is_null(row) || right.is_null(row) {
				return Ok(None);
			}
			let left = left.value(row);
			let right = right.value(row);
			let value = match op {
				ArithmeticOp::Add => left.checked_add(right),
				ArithmeticOp::Sub => left.checked_sub(right),
				ArithmeticOp::Mul => left.checked_mul(right),
				ArithmeticOp::Div => {
					if right == 0 {
						return Ok(None);
					}
					left.checked_div(right)
				}
			}
			.ok_or_else(|| arithmetic_overflow(op, &DataType::Int64))?;
			Ok(Some(value))
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
}

fn evaluate_float32_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_float32(left, "Float32 arithmetic left operand")?;
	let right = as_float32(right, "Float32 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| {
			if left.is_null(row) || right.is_null(row) {
				return None;
			}
			let left = left.value(row);
			let right = right.value(row);
			Some(match op {
				ArithmeticOp::Add => left + right,
				ArithmeticOp::Sub => left - right,
				ArithmeticOp::Mul => left * right,
				ArithmeticOp::Div => left / right,
			})
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
}

fn evaluate_float64_arithmetic(
	op: ArithmeticOp,
	left: &ArrayRef,
	right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
	let left = as_float64(left, "Float64 arithmetic left operand")?;
	let right = as_float64(right, "Float64 arithmetic right operand")?;
	let values = (0..left.len())
		.map(|row| {
			if left.is_null(row) || right.is_null(row) {
				return None;
			}
			let left = left.value(row);
			let right = right.value(row);
			Some(match op {
				ArithmeticOp::Add => left + right,
				ArithmeticOp::Sub => left - right,
				ArithmeticOp::Mul => left * right,
				ArithmeticOp::Div => left / right,
			})
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
}

fn arithmetic_overflow(op: ArithmeticOp, data_type: &DataType) -> ExecutionError {
	ExecutionError::Kernel {
		message: format!(
			"arithmetic overflow: {}({})",
			op.name(),
			arithmetic_data_type_name(data_type)
		),
	}
}

fn arithmetic_data_type_name(data_type: &DataType) -> &'static str {
	match data_type {
		DataType::Int32 => "Int32",
		DataType::Int64 => "Int64",
		DataType::Float32 => "Float32",
		DataType::Float64 => "Float64",
		_ => "Unsupported",
	}
}

fn as_int32<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Int32Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Int32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Int32"),
		})
}

fn as_int64<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Int64Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Int64Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Int64"),
		})
}

fn as_duration_nanos<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a DurationNanosecondArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<DurationNanosecondArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Duration(Nanosecond)"),
		})
}

fn as_interval_year_month<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a IntervalYearMonthArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<IntervalYearMonthArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Interval(YearMonth)"),
		})
}

fn as_float32<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Float32Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Float32Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Float32"),
		})
}

fn as_float64<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a Float64Array, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<Float64Array>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be Float64"),
		})
}

fn as_boolean<'a>(
	array: &'a ArrayRef,
	context: &'static str,
) -> Result<&'a BooleanArray, ExecutionError> {
	array
		.as_any()
		.downcast_ref::<BooleanArray>()
		.ok_or_else(|| ExecutionError::InvalidExpression {
			message: format!("{context} must be boolean"),
		})
}

#[cfg(test)]
mod tests {
	use arrow::datatypes::{Field, Schema};

	#[test]
	fn unbound_time_bucket_matches_the_bound_evaluator() {
		// `evaluate_expr` is `#[cfg(test)]`, so this arm never ran in production —
		// but it carried its own copy of the bucket arithmetic, which truncated
		// toward zero and divided by zero on a zero interval. A test-only
		// evaluator that disagrees with the shipped one lets a test pass against
		// semantics the engine does not have. Both now route through
		// `evaluate_time_bucket`.
		let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
		let batch = RecordBatch::try_new(
			schema,
			vec![Arc::new(Int64Array::from(vec![
				Some(-1_i64),
				Some(-1001),
				None,
			]))],
		)
		.expect("probe batch");
		let executor = Executor::new(Arc::new(crate::storage::InMemoryTableStorage::new()));

		let bucketed = executor
			.evaluate_expr(
				&crate::ir::time_bucket_scalar_function(
					std::time::Duration::from_secs(1),
					Expr::column("ts"),
				),
				&batch,
			)
			.expect("unbound time bucket");
		let values = bucketed
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 output");
		assert_eq!(values.value(0), -1000, "must floor -1, not truncate to 0");
		assert_eq!(values.value(1), -2000, "must floor -1001");
		assert!(values.is_null(2), "null must pass through");

		let zero = executor.evaluate_expr(
			&crate::ir::time_bucket_scalar_function(
				std::time::Duration::from_secs(0),
				Expr::column("ts"),
			),
			&batch,
		);
		assert!(
			zero.is_err(),
			"a zero interval must be refused, not divided by"
		);
	}

	#[test]
	fn exp_propagates_null_and_non_finite_inputs_from_registry_metadata() {
		let input = Arc::new(Float64Array::from(vec![
			Some(0.0),
			Some(f64::NAN),
			Some(f64::INFINITY),
			Some(f64::NEG_INFINITY),
			None,
		])) as ArrayRef;
		let output = evaluate_scalar_function(
			ScalarFunctionId::Exp,
			&[input],
			&crate::ir::ScalarFunctionParameter::None,
			1,
		)
		.expect("registered EXP evaluator");
		let values = output
			.as_any()
			.downcast_ref::<Float64Array>()
			.expect("Float64 output");
		assert_eq!(values.value(0), 1.0);
		assert!(values.value(1).is_nan());
		assert!(values.value(2).is_infinite() && values.value(2).is_sign_positive());
		assert_eq!(values.value(3), 0.0);
		assert!(values.is_null(4));
	}

	#[test]
	fn now_evaluator_repeats_one_bound_instant_for_every_row() {
		let timestamp_nanos = 1_725_000_000_123_456_789_i64;
		let output = evaluate_scalar_function(
			ScalarFunctionId::Now,
			&[],
			&crate::ir::ScalarFunctionParameter::CurrentTimestamp { timestamp_nanos },
			3,
		)
		.expect("registered NOW evaluator");
		assert_eq!(
			output.data_type(),
			&DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
		);
		let values = output
			.as_any()
			.downcast_ref::<TimestampNanosecondArray>()
			.expect("timestamp output");
		assert_eq!(values.values(), &[timestamp_nanos; 3]);
	}

	#[test]
	fn executor_rejects_a_scalar_parameter_on_a_non_temporal_function() {
		let input = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
		let error = evaluate_scalar_function(
			ScalarFunctionId::Abs,
			&[input],
			&crate::ir::ScalarFunctionParameter::Extract(Box::new(
				crate::ir::ExtractScalarFunctionParameter {
					field: crate::ir::TemporalExtractField::Hour,
					zone: None,
				},
			)),
			1,
		)
		.expect_err("ABS must reject a temporal parameter");
		assert!(matches!(
			error,
			ExecutionError::InvalidExpression { message }
				if message == "scalar function has the wrong parameter"
		));
	}
	use super::*;
	#[test]
	fn decimal_division_output_type_validates() {
		// Wide money-style division: (38,8) / (32,4) -> scale = 8+4 = 12,
		// precision = min(38, 38-8+4+12) = 38.
		assert_eq!(decimal_division_output_type(38, 8, 32, 4), Some((38, 12)));
		// (10,2) / (10,2) -> scale 6, precision = min(38, 10-2+2+6) = 16.
		assert_eq!(decimal_division_output_type(10, 2, 10, 2), Some((16, 6)));
		// Over-wide numerator scale: s1=36 -> scale 40 > 38 -> rejected (no silent cap).
		assert_eq!(decimal_division_output_type(38, 36, 10, 2), None);
	}

	#[test]
	fn evaluate_decimal_division_rounds_half_away_and_nulls() {
		let decimal = |values: Vec<Option<i128>>| -> ArrayRef {
			Arc::new(
				Decimal128Array::from(values)
					.with_precision_and_scale(20, 0)
					.expect("decimal operand"),
			)
		};
		// 1 / 20000 = 0.00005 -> at scale 4, half away from zero -> 0.0001 (truncation would be 0.0000).
		let left = decimal(vec![Some(1), Some(1), Some(1), None]);
		let right = decimal(vec![Some(20_000), Some(-20_000), Some(0), Some(5)]);
		let out = evaluate_decimal_division(&left, &right).expect("decimal division");
		let out = out
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal128 result");
		// (20,0) / (20,0) -> scale 4, precision min(38, 20-0+0+4) = 24.
		assert_eq!(out.data_type(), &DataType::Decimal128(24, 4));
		assert_eq!(
			out.value(0),
			1,
			"half away from zero rounds 0.00005 up to 0.0001"
		);
		assert_eq!(
			out.value(1),
			-1,
			"a negative denominator rounds away from zero -> -0.0001"
		);
		assert!(out.is_null(2), "div-by-zero -> NULL (ADR 084)");
		assert!(out.is_null(3), "a NULL operand -> NULL");
	}

	#[test]
	fn numeric_cast_kernels_truncate_preserve_nulls_and_reject_edges() {
		let f64_values =
			Arc::new(Float64Array::from(vec![Some(3.9), Some(-3.9), None])) as ArrayRef;
		let int32_values = evaluate_cast(&f64_values, &DataType::Int32)
			.expect("Float64 to Int32")
			.as_any()
			.downcast_ref::<Int32Array>()
			.expect("Int32 cast")
			.clone();
		assert_eq!(int32_values.value(0), 3);
		assert_eq!(int32_values.value(1), -3);
		assert!(int32_values.is_null(2));

		let int64_values =
			Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN)])) as ArrayRef;
		let float32_values = evaluate_cast(&int64_values, &DataType::Float32)
			.expect("Int64 to Float32")
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("Float32 cast")
			.clone();
		assert!(float32_values.value(0).is_finite());
		assert!(float32_values.value(1).is_finite());

		let special_f64 = Arc::new(Float64Array::from(vec![
			Some(f64::NAN),
			Some(f64::INFINITY),
			Some(f64::NEG_INFINITY),
			None,
		])) as ArrayRef;
		let special_f32 = evaluate_cast(&special_f64, &DataType::Float32)
			.expect("Float64 special values to Float32")
			.as_any()
			.downcast_ref::<Float32Array>()
			.expect("Float32 cast")
			.clone();
		assert!(special_f32.value(0).is_nan());
		assert!(special_f32.value(1).is_infinite());
		assert!(special_f32.value(2).is_infinite());
		assert!(special_f32.is_null(3));

		let i64_overflow =
			Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX) + 1)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&i64_overflow, &DataType::Int32)
				.expect_err("Int64 narrowing overflow should fail"),
			"cast failed: Int64 to Int32:",
		);

		let f64_int_overflow = Arc::new(Float64Array::from(vec![Some(2147483648.0)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_int_overflow, &DataType::Int32)
				.expect_err("Float64 to Int32 exclusive upper bound should fail"),
			"cast failed: Float64 to Int32:",
		);

		let f64_nan = Arc::new(Float64Array::from(vec![Some(f64::NAN)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_nan, &DataType::Int64).expect_err("NaN to Int64 should fail"),
			"cast failed: Float64 to Int64: NaN",
		);

		let f64_float_overflow =
			Arc::new(Float64Array::from(vec![Some((f32::MAX as f64) * 2.0)])) as ArrayRef;
		assert_cast_kernel_prefix(
			evaluate_cast(&f64_float_overflow, &DataType::Float32)
				.expect_err("finite Float64 overflow to Float32 should fail"),
			"cast failed: Float64 to Float32:",
		);
	}

	fn assert_cast_kernel_prefix(err: ExecutionError, expected_prefix: &str) {
		match err {
			ExecutionError::Kernel { message } => assert!(
				message.starts_with(expected_prefix),
				"expected cast kernel prefix {expected_prefix:?}, got {message:?}"
			),
			other => panic!("expected kernel error, got {other:?}"),
		}
	}

	/// `|i32::MIN|` has no `i32`, and wrapping would return the negative input
	/// as though it were a magnitude. The row-level failure has to surface.
	#[test]
	fn abs_reports_overflow_rather_than_wrapping() {
		let input = Arc::new(Int32Array::from(vec![Some(i32::MIN)])) as ArrayRef;
		let err = super::evaluate_abs(&input).expect_err("i32::MIN has no absolute value");
		match err {
			ExecutionError::InvalidExpression { message } => assert!(
				message.contains("ABS overflows"),
				"expected an overflow report, got {message:?}"
			),
			other => panic!("expected an invalid-expression error, got {other:?}"),
		}

		let wrapping = i32::MIN.wrapping_abs();
		assert_eq!(
			wrapping,
			i32::MIN,
			"wrapping_abs returns the input unchanged, which is the value this must never produce"
		);
	}

	/// `GREATEST`/`LEAST` are not ANSI and the dialects disagree: Oracle, MySQL,
	/// and SQLite propagate NULL, PostgreSQL skips it and calls that a
	/// deviation. This engine propagates, so one NULL anywhere in the row
	/// answers NULL — a test that only used non-null rows could not tell the two
	/// dialects apart.
	#[test]
	fn greatest_and_least_propagate_nulls_rather_than_skipping_them() {
		let left = Arc::new(Int64Array::from(vec![Some(3), Some(7), None, Some(1)])) as ArrayRef;
		let middle = Arc::new(Int64Array::from(vec![Some(9), None, None, Some(4)])) as ArrayRef;
		let right =
			Arc::new(Int64Array::from(vec![Some(5), Some(2), Some(8), Some(2)])) as ArrayRef;
		let args = [left, middle, right];

		let greatest =
			super::evaluate_ordered_extreme(&args, super::OrderedExtreme::Greatest, "GREATEST")
				.expect("GREATEST");
		let values = greatest
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 output");
		assert_eq!(values.value(0), 9);
		assert!(
			values.is_null(1),
			"a NULL in any argument answers NULL, not the largest non-null"
		);
		assert!(values.is_null(2));
		assert_eq!(values.value(3), 4);

		let least = super::evaluate_ordered_extreme(&args, super::OrderedExtreme::Least, "LEAST")
			.expect("LEAST");
		let values = least
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 output");
		assert_eq!(values.value(0), 3);
		assert!(values.is_null(1));
		assert_eq!(values.value(3), 1);
	}

	#[test]
	fn greatest_orders_strings_lexicographically_and_takes_more_than_two_arguments() {
		let args = [
			Arc::new(StringArray::from(vec![Some("pear")])) as ArrayRef,
			Arc::new(StringArray::from(vec![Some("apple")])) as ArrayRef,
			Arc::new(StringArray::from(vec![Some("quince")])) as ArrayRef,
			Arc::new(StringArray::from(vec![Some("banana")])) as ArrayRef,
		];
		let greatest =
			super::evaluate_ordered_extreme(&args, super::OrderedExtreme::Greatest, "GREATEST")
				.expect("GREATEST over four strings");
		assert_eq!(
			greatest
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 output")
				.value(0),
			"quince"
		);
	}

	/// The bucket conventions are the standard's: 1-based interior buckets, `0`
	/// for underflow, `count + 1` for overflow, `low` inclusive, `high`
	/// exclusive. Each of those is a place an implementation can silently
	/// disagree with every other database, so each is pinned.
	#[test]
	fn width_bucket_follows_the_ansi_indexing_and_overflow_conventions() {
		let operands = Arc::new(Float64Array::from(vec![
			Some(-1.0),  // below low
			Some(0.0),   // exactly low, first interior bucket
			Some(2.5),   // interior
			Some(9.999), // just below high
			Some(10.0),  // exactly high, overflow
			Some(11.0),  // above high
			None,
		])) as ArrayRef;
		let rows = 7;
		let low = Arc::new(Float64Array::from(vec![Some(0.0); rows])) as ArrayRef;
		let high = Arc::new(Float64Array::from(vec![Some(10.0); rows])) as ArrayRef;
		let count = Arc::new(Int64Array::from(vec![Some(5_i64); rows])) as ArrayRef;

		let output =
			super::evaluate_width_bucket(&operands, &low, &high, &count).expect("WIDTH_BUCKET");
		assert_eq!(output.data_type(), &DataType::Int64);
		let buckets = output
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 output");
		assert_eq!(buckets.value(0), 0, "below low is the underflow bucket");
		assert_eq!(buckets.value(1), 1, "low is inclusive and 1-based");
		assert_eq!(
			buckets.value(2),
			2,
			"2.5 falls in the second of five buckets"
		);
		assert_eq!(buckets.value(3), 5, "just below high is the last bucket");
		assert_eq!(buckets.value(4), 6, "high is exclusive: count + 1");
		assert_eq!(buckets.value(5), 6, "above high is the overflow bucket");
		assert!(buckets.is_null(6), "NULL must propagate");
	}

	#[test]
	fn width_bucket_refuses_a_nonpositive_count_and_a_reversed_range() {
		let operand = Arc::new(Float64Array::from(vec![Some(1.0)])) as ArrayRef;
		let low = Arc::new(Float64Array::from(vec![Some(0.0)])) as ArrayRef;
		let high = Arc::new(Float64Array::from(vec![Some(10.0)])) as ArrayRef;

		let zero = Arc::new(Int64Array::from(vec![Some(0_i64)])) as ArrayRef;
		let err = super::evaluate_width_bucket(&operand, &low, &high, &zero)
			.expect_err("a zero bucket count has no buckets to answer with");
		assert!(format!("{err:?}").contains("positive bucket count"));

		let count = Arc::new(Int64Array::from(vec![Some(5_i64)])) as ArrayRef;
		let err = super::evaluate_width_bucket(&operand, &high, &low, &count)
			.expect_err("a reversed range is not ANSI and must not be guessed at");
		assert!(format!("{err:?}").contains("low < high"));
	}

	/// `CHAR_LENGTH` is a character count. `str::len` would answer in bytes and
	/// agree with it only for ASCII, so the probe uses values where the two
	/// measures differ.
	#[test]
	fn char_length_counts_characters_not_bytes() {
		let input = Arc::new(StringArray::from(vec![
			Some("naïve"),
			Some("日本語"),
			Some("ascii"),
			None,
		])) as ArrayRef;
		let output = super::evaluate_char_length(&input).expect("CHAR_LENGTH");
		assert_eq!(output.data_type(), &DataType::Int64);
		let values = output
			.as_any()
			.downcast_ref::<Int64Array>()
			.expect("Int64 output");
		assert_eq!(values.value(0), 5, "naïve is five characters, six bytes");
		assert_eq!(values.value(1), 3, "three characters, nine bytes");
		assert_eq!(values.value(2), 5);
		assert!(values.is_null(3), "NULL must propagate");
	}

	/// A `LargeUtf8` column must not be narrowed to `Utf8` merely by passing
	/// through a function.
	#[test]
	fn unary_string_preserves_the_input_offset_width() {
		let large = Arc::new(LargeStringArray::from(vec![Some("  hi  ")])) as ArrayRef;
		let trimmed =
			super::evaluate_unary_string(&large, "TRIM", |value| value.trim().to_string())
				.expect("TRIM over LargeUtf8");
		assert_eq!(trimmed.data_type(), &DataType::LargeUtf8);

		let narrow = Arc::new(StringArray::from(vec![Some("  hi  ")])) as ArrayRef;
		let trimmed =
			super::evaluate_unary_string(&narrow, "TRIM", |value| value.trim().to_string())
				.expect("TRIM over Utf8");
		assert_eq!(trimmed.data_type(), &DataType::Utf8);
		assert_eq!(
			trimmed
				.as_any()
				.downcast_ref::<StringArray>()
				.expect("Utf8 output")
				.value(0),
			"hi"
		);
	}

	#[test]
	fn strict_concat_propagates_nulls_and_rejects_mixed_offset_widths() {
		let utf8 = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
		let suffix = Arc::new(StringArray::from(vec![Some("b"), Some("b")])) as ArrayRef;
		let output = super::evaluate_string_concat(&[utf8, suffix]).expect("strict concat");
		let output = output
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Utf8 output");
		assert_eq!(output.value(0), "ab");
		assert!(output.is_null(1), "strict concat must propagate NULL");

		let utf8 = Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef;
		let large = Arc::new(LargeStringArray::from(vec![Some("b")])) as ArrayRef;
		let error = super::evaluate_string_concat(&[utf8, large])
			.expect_err("mixed Utf8/LargeUtf8 must be rejected");
		assert!(format!("{error:?}").contains("share one string type"));
	}

	/// The specification form trims a character set from the selected edge;
	/// treating it as plain whitespace trim would leave the `x` sentinels in
	/// place and would make LEADING and TRAILING indistinguishable.
	#[test]
	fn specification_trim_applies_its_character_set_and_side() {
		let input = Arc::new(StringArray::from(vec![
			Some("xxxalpha xxx"),
			Some("  beta  "),
			None,
		])) as ArrayRef;
		let characters =
			Arc::new(StringArray::from(vec![Some("x"), Some(" "), Some("x")])) as ArrayRef;

		let leading = super::evaluate_trim(
			&[input.clone(), characters.clone()],
			crate::ir::TrimWhere::Leading,
		)
		.expect("TRIM LEADING");
		let leading = leading
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Utf8 output");
		assert_eq!(leading.value(0), "alpha xxx");
		assert_eq!(leading.value(1), "beta  ");
		assert!(leading.is_null(2), "TRIM must propagate NULL input");

		let trailing = super::evaluate_trim(&[input, characters], crate::ir::TrimWhere::Trailing)
			.expect("TRIM TRAILING");
		let trailing = trailing
			.as_any()
			.downcast_ref::<StringArray>()
			.expect("Utf8 output");
		assert_eq!(trailing.value(0), "xxxalpha ");
		assert_eq!(trailing.value(1), "  beta");
		assert!(trailing.is_null(2), "TRIM must propagate NULL input");
	}

	#[test]
	fn abs_preserves_decimal_precision_and_scale() {
		let input = Arc::new(
			Decimal128Array::from(vec![Some(-12_345_i128), None])
				.with_precision_and_scale(15, 2)
				.expect("decimal fixture"),
		) as ArrayRef;
		let output = super::evaluate_abs(&input).expect("decimal ABS");
		assert_eq!(output.data_type(), &DataType::Decimal128(15, 2));
		let values = output
			.as_any()
			.downcast_ref::<Decimal128Array>()
			.expect("decimal output");
		assert_eq!(values.value(0), 12_345);
		assert!(values.is_null(1), "NULL must propagate");
	}

	/// ADR 564: the per-pair conformance walk. Every pair the support table
	/// declares executes in the arms its admission allows; every pair it
	/// excludes fails. An executor arm added without a table declaration, or a
	/// declaration without an arm, fails here — the drift the table exists to
	/// prevent.
	mod adr564_conformance {
		use super::*;
		use crate::cast_support::{CastAdmission, CastFamily, explicit_cast_rule};

		fn representative_type(family: CastFamily) -> DataType {
			match family {
				CastFamily::Int32 => DataType::Int32,
				CastFamily::Int64 => DataType::Int64,
				CastFamily::Float32 => DataType::Float32,
				CastFamily::Float64 => DataType::Float64,
				CastFamily::Decimal128 => DataType::Decimal128(15, 2),
				CastFamily::Utf8 => DataType::Utf8,
				CastFamily::LargeUtf8 => DataType::LargeUtf8,
				CastFamily::Date32 => DataType::Date32,
				CastFamily::Timestamp => {
					crate::timestamp::utc_timestamp_data_type(TimeUnit::Nanosecond)
				}
			}
		}

		/// One convertible value plus a NULL, so every Ok path also proves
		/// NULL propagation. `"12"` parses under every numeric target.
		fn representative_array(family: CastFamily) -> ArrayRef {
			match family {
				CastFamily::Int32 => Arc::new(Int32Array::from(vec![Some(12), None])),
				CastFamily::Int64 => Arc::new(Int64Array::from(vec![Some(12), None])),
				CastFamily::Float32 => Arc::new(Float32Array::from(vec![Some(12.0), None])),
				CastFamily::Float64 => Arc::new(Float64Array::from(vec![Some(12.0), None])),
				CastFamily::Decimal128 => Arc::new(
					Decimal128Array::from(vec![Some(1234), None])
						.with_precision_and_scale(15, 2)
						.expect("decimal fixture"),
				),
				CastFamily::Utf8 => Arc::new(StringArray::from(vec![Some("12"), None])),
				CastFamily::LargeUtf8 => {
					Arc::new(arrow::array::LargeStringArray::from(vec![Some("12"), None]))
				}
				CastFamily::Date32 => Arc::new(Date32Array::from(vec![Some(19_722), None])),
				CastFamily::Timestamp => Arc::new(
					TimestampNanosecondArray::from(vec![Some(0), None]).with_timezone("UTC"),
				),
			}
		}

		#[test]
		fn every_declared_pair_executes_and_every_excluded_pair_fails() {
			for &source in CastFamily::ALL {
				for &target in CastFamily::ALL {
					let source_type = representative_type(source);
					let mut target_type = representative_type(target);
					let rule = explicit_cast_rule(source, target);
					if source == target {
						if rule.is_none() {
							// Identity short-circuits at bind; nothing to walk.
							continue;
						}
						// The one declared intra-family pair rescales decimals.
						target_type = DataType::Decimal128(20, 4);
					}
					assert_ne!(source_type, target_type, "{source:?} -> {target:?}");
					let array = representative_array(source);
					// Text-to-temporal pairs are declared but kernel-routed:
					// the binder rewrites them onto the ADR 558 nodes, so a
					// Cast node must never carry them.
					let kernel_routed = matches!(source, CastFamily::Utf8 | CastFamily::LargeUtf8)
						&& matches!(target, CastFamily::Date32 | CastFamily::Timestamp);
					match rule {
						None => {
							evaluate_cast(&array, &target_type).expect_err(&format!(
								"undeclared pair {source:?} -> {target:?} must fail strict"
							));
							evaluate_try_cast(&array, &target_type).expect_err(&format!(
								"undeclared pair {source:?} -> {target:?} must fail TRY"
							));
						}
						Some(_) if kernel_routed => {
							evaluate_cast(&array, &target_type).expect_err(&format!(
								"kernel-routed pair {source:?} -> {target:?} must not have a strict arm"
							));
							evaluate_try_cast(&array, &target_type).expect_err(&format!(
								"kernel-routed pair {source:?} -> {target:?} must not have a TRY arm"
							));
						}
						Some((_, CastAdmission::SqlCast)) => {
							let strict = evaluate_cast(&array, &target_type).unwrap_or_else(|e| {
								panic!("declared pair {source:?} -> {target:?} failed strict: {e}")
							});
							assert_eq!(strict.data_type(), &target_type);
							assert!(strict.is_null(1), "NULL must propagate strict");
							let lenient =
								evaluate_try_cast(&array, &target_type).unwrap_or_else(|e| {
									panic!("declared pair {source:?} -> {target:?} failed TRY: {e}")
								});
							assert_eq!(lenient.data_type(), &target_type);
							assert!(lenient.is_null(1), "NULL must propagate TRY");
						}
						Some((_, CastAdmission::SynthesizedOnly)) => {
							// Only strict casts are synthesized (arithmetic's
							// `append_cast`); no TRY arm exists or is needed.
							let strict = evaluate_cast(&array, &target_type).unwrap_or_else(|e| {
								panic!("synthesized pair {source:?} -> {target:?} failed: {e}")
							});
							assert_eq!(strict.data_type(), &target_type);
							assert!(strict.is_null(1), "NULL must propagate");
						}
					}
				}
			}
		}
	}
}
