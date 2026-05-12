use std::sync::Arc;
use arrow::array::{ArrayRef, BooleanArray, Int32Array, Float64Array, StringArray};
use arrow::compute::kernels::cmp::{eq, neq, lt, lt_eq, gt, gt_eq};
use arrow::compute::kernels::boolean::{and, or, not, is_null, is_not_null};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use crate::ir::{Expr, LogicalPlan, ScalarValue, AggregateFunction};
use crate::storage::{StorageScan, ScanRequest, TenantId, HlcBound};

#[derive(Debug)]
pub enum ExecutionError {
    Storage(String),
    Compute(String),
    NotImplemented(String),
}

pub struct Executor<'a> {
    storage: &'a dyn StorageScan,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a dyn StorageScan) -> Self {
        Self { storage }
    }

    pub fn execute<'b>(&'b self, plan: &'b LogicalPlan) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, ExecutionError>> + Send + 'b>> {
        Box::pin(async move {
            match plan {
                LogicalPlan::Scan { table, projection } => {
                    let req = ScanRequest {
                        tenant_id: TenantId(0), // Dummy for V0
                        table_id: *table,
                        read_bound: HlcBound(0),
                        projected_columns: projection.clone(),
                        pbc_predicate: None, // PBC lowering deferred to M2
                        profile_hint: None,
                    };
                    let stream = self.storage.scan(req).await
                        .map_err(|e| ExecutionError::Storage(format!("{:?}", e)))?;
                    Ok(stream.batches)
                }
                LogicalPlan::Filter { input, predicate } => {
                    let input_batches = self.execute(input).await?;
                    let mut output = Vec::new();
                    for batch in input_batches {
                        let mask = self.evaluate_expr(predicate, &batch)?;
                        let mask_bool = mask.as_any().downcast_ref::<BooleanArray>()
                            .ok_or_else(|| ExecutionError::Compute("Filter predicate must return boolean".to_string()))?;
                        let filtered = filter_record_batch(&batch, mask_bool)
                            .map_err(|e: arrow::error::ArrowError| ExecutionError::Compute(e.to_string()))?;
                        output.push(filtered);
                    }
                    Ok(output)
                }
                LogicalPlan::Project { input, exprs } => {
                    let input_batches = self.execute(input).await?;
                    let mut output = Vec::new();
                    for batch in input_batches {
                        let mut cols = Vec::new();
                        let mut fields = Vec::new();
                        for (i, expr) in exprs.iter().enumerate() {
                            let array = self.evaluate_expr(expr, &batch)?;
                            cols.push(array.clone());
                            let field = arrow::datatypes::Field::new(
                                format!("col_{}", i),
                                array.data_type().clone(),
                                true, // Simplified
                            );
                            fields.push(field);
                        }
                        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                        let projected = RecordBatch::try_new(schema, cols)
                            .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                        output.push(projected);
                    }
                    Ok(output)
                }
                LogicalPlan::Aggregate { input, aggr_expr } => {
                    let input_batches = self.execute(input).await?;
                    match aggr_expr {
                        AggregateFunction::CountAll => {
                            let mut total: i64 = 0;
                            for batch in input_batches {
                                total += batch.num_rows() as i64;
                            }
                            
                            let array: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![total]));
                            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                                arrow::datatypes::Field::new("count", arrow::datatypes::DataType::Int64, false)
                            ]));
                            let result_batch = RecordBatch::try_new(schema, vec![array])
                                .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                            Ok(vec![result_batch])
                        }
                    }
                }
            }
        })
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, ExecutionError> {
        match expr {
            Expr::Column(name) => {
                let col = batch.column_by_name(name)
                    .ok_or_else(|| ExecutionError::Compute(format!("Column not found: {}", name)))?;
                Ok(col.clone())
            }
            Expr::Literal(val) => {
                let num_rows = batch.num_rows();
                match val {
                    ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
                    ScalarValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
                    ScalarValue::Int64(v) => Ok(Arc::new(arrow::array::Int64Array::from(vec![*v; num_rows]))),
                    ScalarValue::Float32(v) => Ok(Arc::new(arrow::array::Float32Array::from(vec![*v; num_rows]))),
                    ScalarValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; num_rows]))),
                    ScalarValue::Utf8(v) => Ok(Arc::new(StringArray::from(vec![v.clone().as_deref(); num_rows]))),
                }
            }
            Expr::Eq(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = eq(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Neq(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = neq(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Lt(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = lt(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Lte(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = lt_eq(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Gt(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = gt(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Gte(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let res = gt_eq(&left_arr, &right_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::And(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let l_bool = left_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let r_bool = right_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let res = and(l_bool, r_bool)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Or(left, right) => {
                let left_arr = self.evaluate_expr(left, batch)?;
                let right_arr = self.evaluate_expr(right, batch)?;
                let l_bool = left_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let r_bool = right_arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let res = or(l_bool, r_bool)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::Not(expr) => {
                let arr = self.evaluate_expr(expr, batch)?;
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let res = not(bool_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::IsNull(expr) => {
                let arr = self.evaluate_expr(expr, batch)?;
                let res = is_null(&arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::IsNotNull(expr) => {
                let arr = self.evaluate_expr(expr, batch)?;
                let res = is_not_null(&arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
        }
    }
}
