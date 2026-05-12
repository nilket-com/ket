use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, Float64Array, StringArray};
use arrow::compute::kernels::cmp::{eq, neq, lt, lt_eq, gt, gt_eq};
use arrow::compute::kernels::boolean::{and, or, not, is_null, is_not_null};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use crate::ir::{Expr, LogicalPlan, ScalarValue, MorselId, AggregateFunction};
use crate::storage::{StorageScan, ScanRequest, TenantId, HlcBound};
use wendelon::storage::sketch::KllSketch;

#[derive(Debug)]
pub enum ExecutionError {
    Storage(String),
    Compute(String),
    NotImplemented(String),
}

pub struct Executor {
    storage: Arc<dyn StorageScan>,
}

impl Executor {
    pub fn new(storage: Arc<dyn StorageScan>) -> Self {
        Self { storage }
    }

    pub fn execute<'b>(&'b self, plan: &'b LogicalPlan, morsel: Option<(MorselId, u32)>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RecordBatch>, ExecutionError>> + Send + 'b>> {
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
                        morsel,
                    };
                    let stream = self.storage.scan(req).await
                        .map_err(|e| ExecutionError::Storage(format!("{:?}", e)))?;
                    Ok(stream.batches)
                }
                LogicalPlan::Filter { input, predicate } => {
                    let input_batches = self.execute(input, morsel).await?;
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
                    let input_batches = self.execute(input, morsel).await?;
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
                LogicalPlan::Aggregate { input, group_expr, aggr_expr } => {
                    let input_batches = self.execute(input, morsel).await?;
                    if !group_expr.is_empty() {
                        return Err(ExecutionError::NotImplemented("Grouping Aggregate not implemented".to_string()));
                    }

                    // Global aggregation mock
                    let mut results_cols = Vec::new();
                    let mut results_fields = Vec::new();

                    for (func, args) in aggr_expr {
                        match func {
                            AggregateFunction::CountAll => {
                                let mut total: i64 = 0;
                                for batch in &input_batches {
                                    total += batch.num_rows() as i64;
                                }
                                results_cols.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
                                results_fields.push(arrow::datatypes::Field::new("count", arrow::datatypes::DataType::Int64, false));
                            }
                            AggregateFunction::Sum => {
                                let mut total: i64 = 0;
                                for batch in &input_batches {
                                    let arr = self.evaluate_expr(&args[0], batch)?;
                                    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
                                        for i in 0..int_arr.len() {
                                            if !int_arr.is_null(i) {
                                                total += int_arr.value(i) as i64;
                                            }
                                        }
                                    }
                                }
                                results_cols.push(Arc::new(Int64Array::from(vec![total])) as ArrayRef);
                                results_fields.push(arrow::datatypes::Field::new("sum", arrow::datatypes::DataType::Int64, false));
                            }
                            AggregateFunction::LinearRegress => {
                                // Accumulate OLS state: n, sum_x, sum_y, sum_x2, sum_xy
                                let mut n = 0u64;
                                let mut sum_x = 0.0f64;
                                let mut sum_y = 0.0f64;
                                let mut sum_x2 = 0.0f64;
                                let mut sum_xy = 0.0f64;

                                for batch in &input_batches {
                                    let y_arr = self.evaluate_expr(&args[0], batch)?;
                                    let x_arr = self.evaluate_expr(&args[1], batch)?;
                                    let y_f64 = self.cast_to_f64(&y_arr)?;
                                    let x_f64 = self.cast_to_f64(&x_arr)?;

                                    for i in 0..batch.num_rows() {
                                        if !y_f64.is_null(i) && !x_f64.is_null(i) {
                                            let y = y_f64.value(i);
                                            let x = x_f64.value(i);
                                            n += 1;
                                            sum_x += x;
                                            sum_y += y;
                                            sum_x2 += x * x;
                                            sum_xy += x * y;
                                        }
                                    }
                                }

                                if n < 2 {
                                    results_cols.push(Arc::new(Float64Array::from(vec![None, None])) as ArrayRef);
                                } else {
                                    let slope = (n as f64 * sum_xy - sum_x * sum_y) / (n as f64 * sum_x2 - sum_x * sum_x);
                                    let intercept = (sum_y - slope * sum_x) / n as f64;
                                    results_cols.push(Arc::new(Float64Array::from(vec![slope, intercept])) as ArrayRef);
                                }
                                results_fields.push(arrow::datatypes::Field::new("linreg", arrow::datatypes::DataType::Float64, true));
                            }
                            AggregateFunction::ApproxQuantile(q) => {
                                let mut sketch = KllSketch::new(0x04, wendelon::schema::KllK::new(200), 42)
                                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                                
                                for batch in &input_batches {
                                    let arr = self.evaluate_expr(&args[0], batch)?;
                                    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
                                        for i in 0..int_arr.len() {
                                            if !int_arr.is_null(i) {
                                                sketch.insert(int_arr.value(i).to_be_bytes().to_vec());
                                            }
                                        }
                                    }
                                }
                                
                                // Simple estimate from KLL: sort and pick
                                let values = sketch.retained_values();
                                if values.is_empty() {
                                    results_cols.push(Arc::new(Float64Array::from(vec![None])) as ArrayRef);
                                } else {
                                    let mut sorted = values.to_vec();
                                    sorted.sort();
                                    let idx = ((sorted.len() as f64 * q).round() as usize).min(sorted.len() - 1);
                                    let val = i32::from_be_bytes(sorted[idx].clone().try_into().unwrap()) as f64;
                                    results_cols.push(Arc::new(Float64Array::from(vec![val])) as ArrayRef);
                                }
                                results_fields.push(arrow::datatypes::Field::new("quantile", arrow::datatypes::DataType::Float64, true));
                            }
                        }
                    }

                    let schema = Arc::new(arrow::datatypes::Schema::new(results_fields));
                    let result_batch = RecordBatch::try_new(schema, results_cols)
                        .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                    Ok(vec![result_batch])
                }
                LogicalPlan::Exchange { input, partition_count, key } => {
                    let input_batches = self.execute(input, morsel).await?;
                    let mut partitioned_output = Vec::new();
                    
                    for batch in input_batches {
                        let num_rows = batch.num_rows();
                        if num_rows == 0 { continue; }
                        
                        let mut hashes = vec![0u64; num_rows];
                        for expr in key {
                            let arr = self.evaluate_expr(expr, &batch)?;
                            if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
                                for i in 0..num_rows {
                                    if !int_arr.is_null(i) {
                                        hashes[i] ^= int_arr.value(i) as u64;
                                    }
                                }
                            }
                        }
                        
                        let mut builders: Vec<Vec<u32>> = (0..*partition_count).map(|_| Vec::new()).collect();
                        for i in 0..num_rows {
                            let bucket = (hashes[i] % (*partition_count as u64)) as usize;
                            builders[bucket].push(i as u32);
                        }
                        
                        for indices in builders {
                            if !indices.is_empty() {
                                let indices_arr = arrow::array::UInt32Array::from(indices);
                                let mut cols = Vec::new();
                                for i in 0..batch.num_columns() {
                                    let col = batch.column(i);
                                    let taken = arrow::compute::take(col, &indices_arr, None)
                                        .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                                    cols.push(taken);
                                }
                                let partitioned = RecordBatch::try_new(batch.schema(), cols)
                                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                                partitioned_output.push(partitioned);
                            }
                        }
                    }
                    Ok(partitioned_output)
                }
            }
        })
    }

    pub fn evaluate_expr(&self, expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, ExecutionError> {
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
            Expr::Not(inner) => {
                let arr = self.evaluate_expr(inner, batch)?;
                let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                let res = not(bool_arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::IsNull(inner) => {
                let arr = self.evaluate_expr(inner, batch)?;
                let res = is_null(&arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::IsNotNull(inner) => {
                let arr = self.evaluate_expr(inner, batch)?;
                let res = is_not_null(&arr)
                    .map_err(|e| ExecutionError::Compute(e.to_string()))?;
                Ok(Arc::new(res))
            }
            Expr::TimeBucket(interval, inner) => {
                let arr = self.evaluate_expr(inner, batch)?;
                if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                    let interval_ms = interval.as_millis() as i64;
                    let bucketed: Vec<Option<i64>> = int_arr.iter().map(|v| v.map(|v| (v / interval_ms) * interval_ms)).collect();
                    Ok(Arc::new(Int64Array::from(bucketed)) as ArrayRef)
                } else {
                    Err(ExecutionError::Compute("TimeBucket expects Int64 (ms) input".to_string()))
                }
            }
        }
    }

    fn cast_to_f64(&self, arr: &ArrayRef) -> Result<Float64Array, ExecutionError> {
        if let Some(f64_arr) = arr.as_any().downcast_ref::<Float64Array>() {
            return Ok(f64_arr.clone());
        }
        if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
            let values: Vec<Option<f64>> = int_arr.iter().map(|v| v.map(|v| v as f64)).collect();
            return Ok(Float64Array::from(values));
        }
        Err(ExecutionError::Compute(format!("Cannot cast to f64: {:?}", arr.data_type())))
    }
}
