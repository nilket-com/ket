use crate::ir::{ColumnOrdinal, ExecutionExpr, ScalarValue};

// Import OP codes from Wendelon.
use wendelon::schema::pbc::{
	OP_AND, OP_EQ, OP_GT, OP_GTE, OP_IS_NOT_NULL, OP_IS_NULL, OP_LT, OP_LTE, OP_NEQ, OP_NOT, OP_OR,
	OP_PROJECT, OP_PUSH_BOOL, OP_PUSH_COL, OP_PUSH_FLOAT, OP_PUSH_INT, OP_PUSH_STR,
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
		predicate: Option<&ExecutionExpr>,
		projection: Option<&[ColumnOrdinal]>,
	) -> Result<Vec<u8>, PushdownDeclineReason> {
		let mut bytecode = Vec::new();

		if let Some(expr) = predicate {
			self.compile_expr(expr, &mut bytecode)?;
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

	fn compile_expr(
		&self,
		expr: &ExecutionExpr,
		bytecode: &mut Vec<u8>,
	) -> Result<(), PushdownDeclineReason> {
		match expr {
			ExecutionExpr::Column(column) => {
				if !column.pbc_eligible {
					return Err(PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
						ordinal: column.ordinal,
					});
				}
				bytecode.push(OP_PUSH_COL);
				bytecode.extend_from_slice(&column.ordinal.0.to_le_bytes());
			}
			ExecutionExpr::Literal(val) => match val {
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
			},
			ExecutionExpr::Eq(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_EQ);
			}
			ExecutionExpr::Neq(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_NEQ);
			}
			ExecutionExpr::Lt(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_LT);
			}
			ExecutionExpr::Lte(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_LTE);
			}
			ExecutionExpr::Gt(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_GT);
			}
			ExecutionExpr::Gte(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_GTE);
			}
			ExecutionExpr::And(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_AND);
			}
			ExecutionExpr::Or(left, right) => {
				self.compile_expr(left, bytecode)?;
				self.compile_expr(right, bytecode)?;
				bytecode.push(OP_OR);
			}
			ExecutionExpr::Not(inner) => {
				self.compile_expr(inner, bytecode)?;
				bytecode.push(OP_NOT);
			}
			ExecutionExpr::IsNull(inner) => {
				self.compile_expr(inner, bytecode)?;
				bytecode.push(OP_IS_NULL);
			}
			ExecutionExpr::IsNotNull(inner) => {
				self.compile_expr(inner, bytecode)?;
				bytecode.push(OP_IS_NOT_NULL);
			}
			ExecutionExpr::Cast(_, _)
			| ExecutionExpr::TryCast(_, _)
			| ExecutionExpr::Add(_, _)
			| ExecutionExpr::Sub(_, _)
			| ExecutionExpr::Mul(_, _)
			| ExecutionExpr::Div(_, _) => {
				return Err(PushdownDeclineReason::ExpressionShapeUnsupported(
					"arithmetic, Cast, and TryCast are not PBC-eligible".to_string(),
				));
			}
			ExecutionExpr::TimeBucket(_, _) => {
				return Err(PushdownDeclineReason::ExperimentalKernelInPredicate);
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::Executor;
	use crate::ir::{ColumnName, Expr, LogicalPlan, ResolvedColumn, ScalarValue, TableId};
	use crate::storage::InMemoryStorageScan;

	use arrow::array::{Int32Array, StringArray};
	use arrow::datatypes::{DataType, Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::sync::Arc;
	use std::time::Duration;

	#[test]
	fn test_golden_pbc_lowering() {
		let compiler = PbcCompiler::new();

		// expr: amount > 100 AND status == 'active'
		let expr = ExecutionExpr::And(
			Box::new(ExecutionExpr::Gt(
				Box::new(column("amount", 1, DataType::Int32, true)),
				Box::new(ExecutionExpr::Literal(ScalarValue::Int32(Some(100)))),
			)),
			Box::new(ExecutionExpr::Eq(
				Box::new(column("status", 2, DataType::Utf8, true)),
				Box::new(ExecutionExpr::Literal(ScalarValue::Utf8(Some(
					"active".to_string(),
				)))),
			)),
		);

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
	fn test_decline_unsupported_shapes() {
		let compiler = PbcCompiler::new();

		// 1. Column not in the PBC-eligible cascade
		let expr1 = column("payload", 3, DataType::Binary, false);
		let res1 = compiler.compile(Some(&expr1), None);
		assert_eq!(
			res1,
			Err(PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
				ordinal: ColumnOrdinal(3)
			})
		);

		// 2. Null literal (PBC doesn't have OP_PUSH_NULL, nulls are propagated natively)
		let expr2 = ExecutionExpr::Eq(
			Box::new(column("amount", 1, DataType::Int32, true)),
			Box::new(ExecutionExpr::Literal(ScalarValue::Int32(None))),
		);
		let res2 = compiler.compile(Some(&expr2), None);
		assert_eq!(res2, Err(PushdownDeclineReason::NullLiteralUnrepresentable));
	}

	#[test]
	fn test_decline_reasons_are_stable_across_runs() {
		let compiler = PbcCompiler::new();
		let fixtures = vec![
			(
				column("payload", 3, DataType::Binary, false),
				PushdownDeclineReason::ColumnNotInPbcEligibleCascade {
					ordinal: ColumnOrdinal(3),
				},
			),
			(
				ExecutionExpr::Eq(
					Box::new(column("amount", 1, DataType::Int32, true)),
					Box::new(ExecutionExpr::Literal(ScalarValue::Int32(None))),
				),
				PushdownDeclineReason::NullLiteralUnrepresentable,
			),
			(
				ExecutionExpr::TimeBucket(
					Duration::from_secs(60),
					Box::new(column("ts", 4, DataType::Int64, true)),
				),
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
		let expr = ExecutionExpr::And(
			Box::new(ExecutionExpr::Gt(
				Box::new(column("amount", 1, DataType::Int32, true)),
				Box::new(ExecutionExpr::Literal(ScalarValue::Int32(Some(100)))),
			)),
			Box::new(ExecutionExpr::Eq(
				Box::new(column("status", 2, DataType::Utf8, true)),
				Box::new(ExecutionExpr::Literal(ScalarValue::Utf8(Some(
					"active".to_string(),
				)))),
			)),
		);
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

	fn column(
		name: impl Into<String>,
		ordinal: u32,
		data_type: DataType,
		pbc_eligible: bool,
	) -> ExecutionExpr {
		ExecutionExpr::Column(ResolvedColumn {
			name: ColumnName(name.into()),
			ordinal: ColumnOrdinal(ordinal),
			data_type,
			nullable: true,
			pbc_eligible,
		})
	}
}
