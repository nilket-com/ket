use crate::ir::{
	BoundBinaryOp, BoundExprId, BoundExprNode, BoundExprProgram, BoundUnaryOp, ColumnOrdinal,
	ScalarValue,
};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

// Import OP codes from Wendelon.
use wendelon::schema::pbc::{
	OP_AND, OP_EQ, OP_GT, OP_GTE, OP_IN_LIST, OP_IS_NOT_NULL, OP_IS_NULL, OP_LT, OP_LTE, OP_NEQ,
	OP_NOT, OP_OR, OP_PROJECT, OP_PUSH_BOOL, OP_PUSH_COL, OP_PUSH_FLOAT, OP_PUSH_INT, OP_PUSH_STR,
};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PushdownDeclineReason {
	ExpressionShapeUnsupported(String),
	DataTypeNotInPbcCatalog(String),
	ColumnNotInPbcEligibleCascade { ordinal: ColumnOrdinal },
	NullLiteralUnrepresentable,
	NondeterministicExpression,
	ExperimentalKernelInPredicate,
	DescriptorMismatchAtPlanning,
	InternalCompilerError(String),
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone, Copy)]
pub struct PbcCompiler;

#[allow(dead_code)]
impl PbcCompiler {
	pub fn new() -> Self {
		Self
	}

	pub(crate) fn compile(
		&self,
		predicate: Option<&BoundExprProgram>,
		projection: Option<&[ColumnOrdinal]>,
	) -> Result<Vec<u8>, PushdownDeclineReason> {
		let mut bytecode = Vec::new();

		if let Some(predicate) = predicate {
			let root = predicate.single_root().ok_or_else(|| {
				PushdownDeclineReason::ExpressionShapeUnsupported(
					"PBC predicate must have exactly one root".to_string(),
				)
			})?;
			self.compile_node(predicate, root, &mut bytecode)?;
		}

		if let Some(proj) = projection {
			let projection_len = u16::try_from(proj.len()).map_err(|_| {
				PushdownDeclineReason::ExpressionShapeUnsupported(
					"projection column count exceeds u16::MAX".to_string(),
				)
			})?;
			bytecode.push(OP_PROJECT);
			bytecode.extend_from_slice(&projection_len.to_le_bytes());
			for col in proj {
				bytecode.extend_from_slice(&col.0.to_le_bytes());
			}
		}

		Ok(bytecode)
	}

	fn compile_node(
		&self,
		program: &BoundExprProgram,
		root: BoundExprId,
		bytecode: &mut Vec<u8>,
	) -> Result<(), PushdownDeclineReason> {
		let mut stack = vec![(root, false)];
		while let Some((id, emit)) = stack.pop() {
			let node = program.node(id).ok_or_else(|| {
				PushdownDeclineReason::ExpressionShapeUnsupported(format!(
					"bound expression id {:?} out of bounds",
					id
				))
			})?;
			if emit {
				self.emit_node(program, node, bytecode)?;
				continue;
			}
			match node {
				BoundExprNode::Column(_) | BoundExprNode::Literal(_) => stack.push((id, true)),
				BoundExprNode::NullLiteral => {
					return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
						"NULL literal is not PBC-eligible".to_string(),
					));
				}
				BoundExprNode::Binary { op, left, right } => {
					if binary_has_fixed_duration_interval_type(program, Some(id), *left, *right) {
						return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
							"fixed-duration interval arithmetic is not PBC-eligible".to_string(),
						));
					}
					if binary_has_calendar_month_interval_type(program, Some(id), *left, *right) {
						return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
							"calendar interval arithmetic is not PBC-eligible".to_string(),
						));
					}
					if matches!(
						op,
						BoundBinaryOp::Add
							| BoundBinaryOp::Sub | BoundBinaryOp::Mul
							| BoundBinaryOp::Div
					) {
						return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
							"arithmetic, Cast, and TryCast are not PBC-eligible".to_string(),
						));
					}
					stack.push((id, true));
					stack.push((*right, false));
					stack.push((*left, false));
				}
				BoundExprNode::Unary { input, .. } => {
					stack.push((id, true));
					stack.push((*input, false));
				}
				BoundExprNode::InList { expr, list, .. } => {
					if list.is_empty() {
						return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
							"IN list must not be empty".to_string(),
						));
					}
					u16::try_from(list.len()).map_err(|_| {
						PushdownDeclineReason::ExpressionShapeUnsupported(
							"IN list item count exceeds u16::MAX".to_string(),
						)
					})?;
					stack.push((id, true));
					for item in list.iter().rev() {
						stack.push((*item, false));
					}
					stack.push((*expr, false));
				}
				BoundExprNode::Cast { .. } | BoundExprNode::TryCast { .. } => {
					return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
						"arithmetic, Cast, and TryCast are not PBC-eligible".to_string(),
					));
				}
				BoundExprNode::Like { .. } => {
					return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
						"LIKE is not PBC-eligible".to_string(),
					));
				}
				BoundExprNode::Case { .. } => {
					return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
						"CASE is not PBC-eligible".to_string(),
					));
				}
				BoundExprNode::TimeBucket { .. } => {
					return Err(PushdownDeclineReason::ExperimentalKernelInPredicate);
				}
				BoundExprNode::AtTimeZone { .. }
				| BoundExprNode::Extract { .. }
				| BoundExprNode::DateTrunc { .. } => {
					return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
						"session-zoned temporal operations are not PBC-eligible".to_string(),
					));
				}
			}
		}
		Ok(())
	}

	fn emit_node(
		&self,
		program: &BoundExprProgram,
		node: &BoundExprNode,
		bytecode: &mut Vec<u8>,
	) -> Result<(), PushdownDeclineReason> {
		match node {
			BoundExprNode::Column(column) => {
				if !column.pbc_eligible {
					return Err(PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
						ordinal: column.ordinal,
					});
				}
				bytecode.push(OP_PUSH_COL);
				bytecode.extend_from_slice(&column.ordinal.0.to_le_bytes());
			}
			BoundExprNode::Literal(literal) => {
				let val = program.literals.get(*literal as usize).ok_or_else(|| {
					PushdownDeclineReason::ExpressionShapeUnsupported(format!(
						"literal slot {literal} out of bounds"
					))
				})?;
				emit_literal(val, bytecode)?;
			}
			BoundExprNode::NullLiteral => {
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"NULL literal reached PBC emitter".to_string(),
				));
			}
			BoundExprNode::Binary { left, right, .. }
				if binary_has_fixed_duration_interval_type(program, None, *left, *right) =>
			{
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"fixed-duration interval arithmetic is not PBC-eligible".to_string(),
				));
			}
			BoundExprNode::Binary { left, right, .. }
				if binary_has_calendar_month_interval_type(program, None, *left, *right) =>
			{
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"calendar interval arithmetic is not PBC-eligible".to_string(),
				));
			}
			BoundExprNode::Binary { op, .. } => bytecode.push(binary_opcode(op)?),
			BoundExprNode::Unary { op, .. } => bytecode.push(unary_opcode(op)),
			BoundExprNode::InList { list, negated, .. } => {
				let count = u16::try_from(list.len()).map_err(|_| {
					PushdownDeclineReason::ExpressionShapeUnsupported(
						"IN list item count exceeds u16::MAX".to_string(),
					)
				})?;
				bytecode.push(OP_IN_LIST);
				bytecode.extend_from_slice(&count.to_le_bytes());
				if *negated {
					bytecode.push(OP_NOT);
				}
			}
			BoundExprNode::Cast { .. }
			| BoundExprNode::TryCast { .. }
			| BoundExprNode::Like { .. }
			| BoundExprNode::Case { .. }
			| BoundExprNode::TimeBucket { .. }
			| BoundExprNode::AtTimeZone { .. }
			| BoundExprNode::Extract { .. }
			| BoundExprNode::DateTrunc { .. } => {
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"unsupported node reached PBC emitter".to_string(),
				));
			}
		}
		Ok(())
	}
}

fn binary_has_fixed_duration_interval_type(
	program: &BoundExprProgram,
	output: Option<BoundExprId>,
	left: BoundExprId,
	right: BoundExprId,
) -> bool {
	output
		.and_then(|id| program.node_type(id))
		.is_some_and(is_fixed_duration_interval_type)
		|| program
			.node_type(left)
			.is_some_and(is_fixed_duration_interval_type)
		|| program
			.node_type(right)
			.is_some_and(is_fixed_duration_interval_type)
}

fn is_fixed_duration_interval_type(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Duration(TimeUnit::Nanosecond))
}

fn binary_has_calendar_month_interval_type(
	program: &BoundExprProgram,
	output: Option<BoundExprId>,
	left: BoundExprId,
	right: BoundExprId,
) -> bool {
	output
		.and_then(|id| program.node_type(id))
		.is_some_and(is_calendar_month_interval_type)
		|| program
			.node_type(left)
			.is_some_and(is_calendar_month_interval_type)
		|| program
			.node_type(right)
			.is_some_and(is_calendar_month_interval_type)
}

fn is_calendar_month_interval_type(data_type: &DataType) -> bool {
	matches!(data_type, DataType::Interval(IntervalUnit::YearMonth))
}

fn emit_literal(val: &ScalarValue, bytecode: &mut Vec<u8>) -> Result<(), PushdownDeclineReason> {
	match val {
		ScalarValue::Boolean(Some(v)) => {
			bytecode.push(OP_PUSH_BOOL);
			bytecode.push(u8::from(*v));
		}
		ScalarValue::Int32(Some(v)) => {
			bytecode.push(OP_PUSH_INT);
			bytecode.extend_from_slice(&(*v as i64).to_le_bytes());
		}
		ScalarValue::Int64(Some(v)) => {
			bytecode.push(OP_PUSH_INT);
			bytecode.extend_from_slice(&v.to_le_bytes());
		}
		ScalarValue::Date32(Some(_)) => {
			return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"Date32 literals require type-preserving PBC support".to_string(),
			));
		}
		ScalarValue::Timestamp { value: Some(_), .. } => {
			return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"Timestamp literals require type-preserving PBC support".to_string(),
			));
		}
		ScalarValue::Interval(Some(_)) => {
			return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"fixed-duration interval literals are not PBC-eligible".to_string(),
			));
		}
		ScalarValue::CalendarMonthInterval(Some(_)) => {
			return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"calendar interval literals are not PBC-eligible".to_string(),
			));
		}
		ScalarValue::Float32(Some(v)) => {
			bytecode.push(OP_PUSH_FLOAT);
			bytecode.extend_from_slice(&(*v as f64).to_le_bytes());
		}
		ScalarValue::Float64(Some(v)) => {
			bytecode.push(OP_PUSH_FLOAT);
			bytecode.extend_from_slice(&v.to_le_bytes());
		}
		ScalarValue::Utf8(Some(v)) => {
			let bytes = v.as_bytes();
			if bytes.len() > u16::MAX as usize {
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"String literal too long".to_string(),
				));
			}
			bytecode.push(OP_PUSH_STR);
			bytecode.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
			bytecode.extend_from_slice(bytes);
		}
		_ => return Err(PushdownDeclineReason::NullLiteralUnrepresentable),
	}
	Ok(())
}

fn binary_opcode(op: &BoundBinaryOp) -> Result<u8, PushdownDeclineReason> {
	match op {
		BoundBinaryOp::Eq => Ok(OP_EQ),
		BoundBinaryOp::Neq => Ok(OP_NEQ),
		BoundBinaryOp::Lt => Ok(OP_LT),
		BoundBinaryOp::Lte => Ok(OP_LTE),
		BoundBinaryOp::Gt => Ok(OP_GT),
		BoundBinaryOp::Gte => Ok(OP_GTE),
		BoundBinaryOp::And => Ok(OP_AND),
		BoundBinaryOp::Or => Ok(OP_OR),
		BoundBinaryOp::Add | BoundBinaryOp::Sub | BoundBinaryOp::Mul | BoundBinaryOp::Div => {
			Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"arithmetic, Cast, and TryCast are not PBC-eligible".to_string(),
			))
		}
	}
}

fn unary_opcode(op: &BoundUnaryOp) -> u8 {
	match op {
		BoundUnaryOp::Not => OP_NOT,
		BoundUnaryOp::IsNull => OP_IS_NULL,
		BoundUnaryOp::IsNotNull => OP_IS_NOT_NULL,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::Executor;
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, ColumnName, Expr, LogicalPlan, ResolvedColumn,
		ScalarValue, TableId,
	};
	use crate::storage::InMemoryStorageScan;

	use arrow::array::{Int32Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
	use arrow::record_batch::RecordBatch;
	use std::sync::Arc;
	use std::time::Duration;

	#[test]
	fn test_golden_pbc_lowering() {
		let compiler = PbcCompiler::new();

		// expr: amount > 100 AND status == 'active'
		let expr = pbc_fixture_expr();

		let bytecode = compiler.compile(Some(&expr), None).unwrap();

		let mut expected = Vec::new();

		// Gt
		expected.push(OP_PUSH_COL);
		expected.extend_from_slice(&1u32.to_le_bytes()); // 'amount' is 1
		expected.push(OP_PUSH_INT);
		expected.extend_from_slice(&100i64.to_le_bytes()); // 100
		expected.push(OP_GT);

		// Eq
		expected.push(OP_PUSH_COL);
		expected.extend_from_slice(&2u32.to_le_bytes()); // 'status' is 2
		expected.push(OP_PUSH_STR);
		expected.extend_from_slice(&6u16.to_le_bytes()); // len 6
		expected.extend_from_slice(b"active");
		expected.push(OP_EQ);

		// And
		expected.push(OP_AND);

		assert_eq!(bytecode, expected);
	}

	#[test]
	fn in_list_lowers_to_native_opcode_with_target_then_members() {
		let compiler = PbcCompiler::new();
		let expr = program(|builder| {
			let status = column(builder, "status", 2, DataType::Utf8, true);
			let active = builder.append_literal(ScalarValue::Utf8(Some("active".to_string())));
			let pending = builder.append_literal(ScalarValue::Utf8(Some("pending".to_string())));
			builder.append_in_list(status, vec![active, pending], false)
		});

		let bytecode = compiler.compile(Some(&expr), None).unwrap();

		let mut expected = Vec::new();
		expected.push(OP_PUSH_COL);
		expected.extend_from_slice(&2u32.to_le_bytes());
		expected.push(OP_PUSH_STR);
		expected.extend_from_slice(&6u16.to_le_bytes());
		expected.extend_from_slice(b"active");
		expected.push(OP_PUSH_STR);
		expected.extend_from_slice(&7u16.to_le_bytes());
		expected.extend_from_slice(b"pending");
		expected.push(OP_IN_LIST);
		expected.extend_from_slice(&2u16.to_le_bytes());

		assert_eq!(bytecode, expected);
		assert!(wendelon::schema::pbc::Program::compile(&bytecode).is_ok());
	}

	#[test]
	fn not_in_list_lowers_to_native_opcode_plus_not() {
		let compiler = PbcCompiler::new();
		let expr = program(|builder| {
			let status = column(builder, "status", 2, DataType::Utf8, true);
			let active = builder.append_literal(ScalarValue::Utf8(Some("active".to_string())));
			builder.append_in_list(status, vec![active], true)
		});

		let bytecode = compiler.compile(Some(&expr), None).unwrap();

		assert_eq!(*bytecode.last().expect("not opcode"), OP_NOT);
		assert!(bytecode.contains(&OP_IN_LIST));
		assert!(wendelon::schema::pbc::Program::compile(&bytecode).is_ok());
	}

	#[test]
	fn in_list_at_adr074_bound_stays_within_wendelon_pbc_limits() {
		let compiler = PbcCompiler::new();
		let expr = program(|builder| {
			let amount = column(builder, "amount", 1, DataType::Int32, true);
			let items = (0..32)
				.map(|value| builder.append_literal(ScalarValue::Int32(Some(value))))
				.collect::<Vec<_>>();
			builder.append_in_list(amount, items, false)
		});

		let bytecode = compiler.compile(Some(&expr), None).unwrap();

		assert!(bytecode.contains(&OP_IN_LIST));
		wendelon::schema::pbc::Program::compile(&bytecode)
			.expect("32-item IN_LIST must be stack-safe for Wendelon PBC");
	}

	#[test]
	fn test_decline_unsupported_shapes() {
		let compiler = PbcCompiler::new();

		// 1. Column not in the PBC-eligible cascade
		let expr1 = column_program("payload", 3, DataType::Binary, false);
		let res1 = compiler.compile(Some(&expr1), None);
		assert_eq!(
			res1,
			Err(PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
				ordinal: ColumnOrdinal(3)
			})
		);

		// 2. Null literal (PBC doesn't have OP_PUSH_NULL, nulls are propagated natively)
		let expr2 = program(|builder| {
			let amount = column(builder, "amount", 1, DataType::Int32, true);
			let value = builder.append_literal(ScalarValue::Int32(None));
			builder.append_binary(BoundBinaryOp::Eq, amount, value, DataType::Boolean)
		});
		let res2 = compiler.compile(Some(&expr2), None);
		assert_eq!(res2, Err(PushdownDeclineReason::NullLiteralUnrepresentable));

		// 3. Timestamp literals are not raw Int64 PBC operands.
		let expr3 = program(|builder| {
			let ts = column(builder, "observed_at_ticks", 4, DataType::Int64, true);
			let value = builder.append_literal(ScalarValue::Timestamp {
				value: Some(1_700_000_000_000),
				unit: TimeUnit::Millisecond,
			});
			builder.append_binary(BoundBinaryOp::Eq, ts, value, DataType::Boolean)
		});
		let res3 = compiler.compile(Some(&expr3), None);
		assert_eq!(
			res3,
			Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"Timestamp literals require type-preserving PBC support".to_string()
			))
		);

		// 4. Fixed-duration interval operands are not in the PBC type catalog.
		let expr4 = program(|builder| {
			let duration = column(
				builder,
				"elapsed",
				5,
				DataType::Duration(TimeUnit::Nanosecond),
				true,
			);
			let value = builder.append_literal(ScalarValue::Interval(Some(1_000_000_000)));
			builder.append_binary(BoundBinaryOp::Gt, duration, value, DataType::Boolean)
		});
		let res4 = compiler.compile(Some(&expr4), None);
		assert_eq!(
			res4,
			Err(PushdownDeclineReason::ExpressionShapeUnsupported(
				"fixed-duration interval arithmetic is not PBC-eligible".to_string()
			))
		);
	}

	#[test]
	fn test_decline_reasons_are_stable_across_runs() {
		let compiler = PbcCompiler::new();
		let fixtures = vec![
			(
				column_program("payload", 3, DataType::Binary, false),
				PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
					ordinal: ColumnOrdinal(3),
				},
			),
			(
				program(|builder| {
					let amount = column(builder, "amount", 1, DataType::Int32, true);
					let value = builder.append_literal(ScalarValue::Int32(None));
					builder.append_binary(BoundBinaryOp::Eq, amount, value, DataType::Boolean)
				}),
				PushdownDeclineReason::NullLiteralUnrepresentable,
			),
			(
				program(|builder| {
					let ts = column(builder, "ts", 4, DataType::Int64, true);
					builder.append_time_bucket(Duration::from_secs(60), ts, DataType::Int64)
				}),
				PushdownDeclineReason::ExperimentalKernelInPredicate,
			),
		];

		for (expr, expected) in fixtures {
			assert_eq!(compiler.compile(Some(&expr), None), Err(expected.clone()));
			assert_eq!(compiler.compile(Some(&expr), None), Err(expected));
		}
	}

	#[tokio::test]
	async fn test_parity_against_in_memory_execution() {
		// Create RecordBatch fixture
		let schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int32, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("status", DataType::Utf8, true),
		]));

		let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
		let amount_array = Int32Array::from(vec![Some(50), Some(150), None, Some(200), Some(80)]);
		let status_array = StringArray::from(vec![
			Some("active"),
			Some("active"),
			Some("inactive"),
			Some("active"),
			Some("pending"),
		]);

		let batch = RecordBatch::try_new(
			schema,
			vec![
				Arc::new(id_array),
				Arc::new(amount_array),
				Arc::new(status_array),
			],
		)
		.unwrap();

		let plan = LogicalPlan::Filter {
			predicate: Expr::And(
				Box::new(Expr::Gt(
					Box::new(Expr::column("amount")),
					Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
				)),
				Box::new(Expr::Eq(
					Box::new(Expr::column("status")),
					Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
				)),
			),
			input: Box::new(LogicalPlan::Scan {
				table: TableId(1),
				projection: None,
			}),
		};

		let storage = Arc::new(InMemoryStorageScan {
			table_data: batch.clone(),
		});
		let executor = Executor::new(storage);
		let executor_results = executor.execute(&plan, None).await.unwrap();
		let executor_result_batch = &executor_results[0];

		// 2. Wendelon PBC Engine
		let expr = pbc_fixture_expr();
		let compiler = PbcCompiler::new();
		let bytecode = compiler.compile(Some(&expr), None).unwrap();

		let wendelon_result_batch = wendelon::schema::pbc::execute(&bytecode, &batch).unwrap();

		// 3. Parity Check
		assert_eq!(
			executor_result_batch.num_rows(),
			wendelon_result_batch.num_rows()
		);

		let exec_amount = executor_result_batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.unwrap();
		let pbc_amount = wendelon_result_batch
			.column(1)
			.as_any()
			.downcast_ref::<Int32Array>()
			.unwrap();

		assert_eq!(exec_amount.len(), pbc_amount.len());
		for i in 0..exec_amount.len() {
			assert_eq!(exec_amount.value(i), pbc_amount.value(i));
		}
	}

	fn pbc_fixture_expr() -> BoundExprProgram {
		program(|builder| {
			let amount = column(builder, "amount", 1, DataType::Int32, true);
			let amount_threshold = builder.append_literal(ScalarValue::Int32(Some(100)));
			let amount_gt_threshold = builder.append_binary(
				BoundBinaryOp::Gt,
				amount,
				amount_threshold,
				DataType::Boolean,
			);

			let status = column(builder, "status", 2, DataType::Utf8, true);
			let active = builder.append_literal(ScalarValue::Utf8(Some("active".to_string())));
			let status_is_active =
				builder.append_binary(BoundBinaryOp::Eq, status, active, DataType::Boolean);

			builder.append_binary(
				BoundBinaryOp::And,
				amount_gt_threshold,
				status_is_active,
				DataType::Boolean,
			)
		})
	}

	fn column_program(
		name: impl Into<String>,
		ordinal: u32,
		data_type: DataType,
		pbc_eligible: bool,
	) -> BoundExprProgram {
		program(|builder| column(builder, name, ordinal, data_type, pbc_eligible))
	}

	fn program(root: impl FnOnce(&mut BoundExprProgramBuilder) -> BoundExprId) -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let root = root(&mut builder);
		builder.push_root(root);
		builder.build()
	}

	fn column(
		builder: &mut BoundExprProgramBuilder,
		name: impl Into<String>,
		ordinal: u32,
		data_type: DataType,
		pbc_eligible: bool,
	) -> BoundExprId {
		builder.append_column(ResolvedColumn {
			name: ColumnName(name.into()),
			ordinal: ColumnOrdinal(ordinal),
			data_type,
			nullable: true,
			pbc_eligible,
		})
	}
}
