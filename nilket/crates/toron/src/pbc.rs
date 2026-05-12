use std::collections::HashMap;
use crate::ir::{Expr, ScalarValue};

// Import OP codes from Wendelon.
use wendelon::schema::pbc::{
    OP_PUSH_COL, OP_PUSH_INT, OP_PUSH_STR, OP_PUSH_BOOL, OP_PUSH_FLOAT,
    OP_EQ, OP_NEQ, OP_GT, OP_LT, OP_GTE, OP_LTE,
    OP_AND, OP_OR, OP_NOT, OP_IS_NULL, OP_IS_NOT_NULL, OP_PROJECT
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoweringDeclineReason {
    UnsupportedExpressionShape(String),
    ColumnNotFound(String),
    NullLiteral,
}

pub struct PbcCompiler {
    column_indices: HashMap<String, u32>,
}

impl PbcCompiler {
    pub fn new(column_indices: HashMap<String, u32>) -> Self {
        Self { column_indices }
    }

    pub fn compile(
        &self, 
        predicate: Option<&Expr>, 
        projection: Option<&[u32]>
    ) -> Result<Vec<u8>, LoweringDeclineReason> {
        let mut bytecode = Vec::new();
        
        if let Some(expr) = predicate {
            self.compile_expr(expr, &mut bytecode)?;
        }
        
        if let Some(proj) = projection {
            bytecode.push(OP_PROJECT);
            bytecode.extend_from_slice(&(proj.len() as u16).to_le_bytes());
            for &col in proj {
                bytecode.extend_from_slice(&col.to_le_bytes());
            }
        }
        
        Ok(bytecode)
    }

    fn compile_expr(&self, expr: &Expr, bytecode: &mut Vec<u8>) -> Result<(), LoweringDeclineReason> {
        match expr {
            Expr::Column(name) => {
                let idx = self.column_indices.get(name)
                    .ok_or_else(|| LoweringDeclineReason::ColumnNotFound(name.clone()))?;
                bytecode.push(OP_PUSH_COL);
                bytecode.extend_from_slice(&idx.to_le_bytes());
            }
            Expr::Literal(val) => {
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
                            return Err(LoweringDeclineReason::UnsupportedExpressionShape("String literal too long".to_string()));
                        }
                        bytecode.push(OP_PUSH_STR);
                        bytecode.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                        bytecode.extend_from_slice(bytes);
                    }
                    _ => return Err(LoweringDeclineReason::NullLiteral),
                }
            }
            Expr::Eq(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_EQ);
            }
            Expr::Neq(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_NEQ);
            }
            Expr::Lt(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_LT);
            }
            Expr::Lte(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_LTE);
            }
            Expr::Gt(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_GT);
            }
            Expr::Gte(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_GTE);
            }
            Expr::And(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_AND);
            }
            Expr::Or(left, right) => {
                self.compile_expr(left, bytecode)?;
                self.compile_expr(right, bytecode)?;
                bytecode.push(OP_OR);
            }
            Expr::Not(inner) => {
                self.compile_expr(inner, bytecode)?;
                bytecode.push(OP_NOT);
            }
            Expr::IsNull(inner) => {
                self.compile_expr(inner, bytecode)?;
                bytecode.push(OP_IS_NULL);
            }
            Expr::IsNotNull(inner) => {
                self.compile_expr(inner, bytecode)?;
                bytecode.push(OP_IS_NOT_NULL);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Expr, ScalarValue, LogicalPlan, TableId};
    use crate::exec::Executor;
    use crate::storage::InMemoryStorageScan;
    
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn test_schema() -> HashMap<String, u32> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), 0);
        map.insert("amount".to_string(), 1);
        map.insert("status".to_string(), 2);
        map
    }

    #[test]
    fn test_golden_pbc_lowering() {
        let compiler = PbcCompiler::new(test_schema());

        // expr: amount > 100 AND status == 'active'
        let expr = Expr::And(
            Box::new(Expr::Gt(
                Box::new(Expr::Column("amount".to_string())),
                Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
            )),
            Box::new(Expr::Eq(
                Box::new(Expr::Column("status".to_string())),
                Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
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
        let compiler = PbcCompiler::new(test_schema());

        // 1. Column not found
        let expr1 = Expr::Column("non_existent".to_string());
        let res1 = compiler.compile(Some(&expr1), None);
        assert_eq!(res1, Err(LoweringDeclineReason::ColumnNotFound("non_existent".to_string())));

        // 2. Null literal (PBC doesn't have OP_PUSH_NULL, nulls are propagated natively)
        let expr2 = Expr::Eq(
            Box::new(Expr::Column("amount".to_string())),
            Box::new(Expr::Literal(ScalarValue::Int32(None))),
        );
        let res2 = compiler.compile(Some(&expr2), None);
        assert_eq!(res2, Err(LoweringDeclineReason::NullLiteral));
    }

    #[test]
    fn test_parity_against_in_memory_execution() {
        // Create RecordBatch fixture
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("status", DataType::Utf8, true),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let amount_array = Int32Array::from(vec![Some(50), Some(150), None, Some(200), Some(80)]);
        let status_array = StringArray::from(vec![Some("active"), Some("active"), Some("inactive"), Some("active"), Some("pending")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(amount_array),
                Arc::new(status_array),
            ],
        ).unwrap();

        // 1. Toron Native Executor (M1 logic)
        let plan = LogicalPlan::Filter {
            predicate: Expr::And(
                Box::new(Expr::Gt(
                    Box::new(Expr::Column("amount".to_string())),
                    Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
                )),
                Box::new(Expr::Eq(
                    Box::new(Expr::Column("status".to_string())),
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
                )),
            ),
            input: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: None,
            }),
        };

        let storage = InMemoryStorageScan { table_data: batch.clone() };
        let executor = Executor::new(&storage);
        let executor_results = executor.execute(&plan).unwrap();
        let executor_result_batch = &executor_results[0];

        // 2. Wendelon PBC Engine
        let expr = Expr::And(
            Box::new(Expr::Gt(
                Box::new(Expr::Column("amount".to_string())),
                Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
            )),
            Box::new(Expr::Eq(
                Box::new(Expr::Column("status".to_string())),
                Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
            )),
        );
        let compiler = PbcCompiler::new(test_schema());
        let bytecode = compiler.compile(Some(&expr), None).unwrap();

        let wendelon_result_batch = wendelon::schema::pbc::execute(&bytecode, &batch).unwrap();

        // 3. Parity Check
        assert_eq!(executor_result_batch.num_rows(), wendelon_result_batch.num_rows());
        
        let exec_amount = executor_result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        let pbc_amount = wendelon_result_batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        
        assert_eq!(exec_amount.len(), pbc_amount.len());
        for i in 0..exec_amount.len() {
            assert_eq!(exec_amount.value(i), pbc_amount.value(i));
        }
    }
}
